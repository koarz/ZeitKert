#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "common/ZeitgeistDB.hpp"
#include "linenoise.h"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <string>
TEST(create_database, create_new_database) {
  DB::ZeitgeistDB db;

  DB::ResultSet res;

  // Correctly cleans up data when a test has an error or completes properly
  std::unique_ptr<int, std::function<void(int *)>> p(
      new int(0), [&db, &res](int *ptr) {
        std::string query = "drop database TestDb;";
        auto status = db.ExecuteQuery(query, res);
        delete ptr;
      });
  {
    std::string query = "create database TestDb;";
    DB::Status status = db.ExecuteQuery(query, res);
    ASSERT_TRUE(status.ok());
  }
}
TEST(create_database, create_database_error_for_exit) {
  DB::ZeitgeistDB db;

  DB::ResultSet res;
  std::unique_ptr<int, std::function<void(int *)>> p(
      new int(0), [&db, &res](int *ptr) {
        std::string query = "drop database TestDb;";

        auto status = db.ExecuteQuery(query, res);
        delete ptr;
      });
  {
    std::string query = "create database TestDb;";

    DB::Status status = db.ExecuteQuery(query, res);
    ASSERT_TRUE(status.ok());
    query = "create database TestDb;";
    status = db.ExecuteQuery(query, res);
    ASSERT_EQ(status.GetMessage(), "The Database Already Exists");
  }
}