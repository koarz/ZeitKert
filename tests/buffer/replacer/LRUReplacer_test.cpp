#include "buffer/replacer/LRUReplacer.hpp"
#include "common/Config.hpp"

#include <gtest/gtest.h>

TEST(LRUReplacerTest, SampleTest) {
  const int frame_num = 4;
  DB::LRUReplacer replacer(frame_num);
  frame_id_t evict;

  for (int i = 0; i < frame_num; i++) {
    replacer.Evict(&evict);
    ASSERT_EQ(i, evict);
    replacer.Access(evict);
  }
  // all frames pin 1
  for (int i = 0; i < frame_num; i++) {
    replacer.Evict(&evict);
    ASSERT_EQ(-1, evict);
  }
  // unpin those frames
  for (int i = 0; i < frame_num; i++) {
    replacer.UnPin(i);
  }
  // frame access twice
  for (int i = 0; i < frame_num; i++) {
    replacer.Evict(&evict);
    ASSERT_EQ(i, evict);
    replacer.Access(evict);
    replacer.Access(evict);
  }
  // unpin evict get -1
  for (int i = 0; i < frame_num; i++) {
    replacer.UnPin(i);
    replacer.Evict(&evict);
    ASSERT_EQ(-1, evict);
  }
  // unpin evict get i
  for (int i = 0; i < frame_num; i++) {
    replacer.UnPin(i);
    replacer.Evict(&evict);
    ASSERT_EQ(i, evict);
    replacer.Access(i);
  }
  for (int i = 0; i < frame_num; i++) {
    replacer.UnPin(i);
  }
  // half frame evict and access
  for (int i = 0; i < frame_num / 2; i++) {
    replacer.Evict(&evict);
    ASSERT_EQ(i, evict);
    replacer.Access(evict);
  }
  // another frame evicted
  for (int i = frame_num / 2; i < frame_num; i++) {
    replacer.Evict(&evict);
    ASSERT_EQ(i, evict);
    replacer.Access(evict);
  }
}