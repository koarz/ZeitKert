#include "storage/MMapFile.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace DB {
MMapFile::MMapFile(const std::filesystem::path &path) {
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) {
    return;
  }
  struct stat st;
  if (::fstat(fd_, &st) != 0) {
    ::close(fd_);
    fd_ = -1;
    return;
  }
  size_ = static_cast<size_t>(st.st_size);
  if (size_ == 0) {
    ::close(fd_);
    fd_ = -1;
    return;
  }
  void *mapped = ::mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd_, 0);
  if (mapped == MAP_FAILED) {
    ::close(fd_);
    fd_ = -1;
    return;
  }
  data_ = reinterpret_cast<Byte *>(mapped);
}

MMapFile::~MMapFile() {
  if (data_ && data_ != MAP_FAILED) {
    ::munmap(data_, size_);
  }
  if (fd_ >= 0) {
    ::close(fd_);
  }
}
} // namespace DB
