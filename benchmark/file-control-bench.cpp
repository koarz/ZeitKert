#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <unistd.h>

constexpr size_t DATA_SIZE = 1'073'741'824; // 1GB

void RandomData(char *data, size_t size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, 0xff);
  for (int i = 0; i < size; i++) {
    data[i] = static_cast<char>(distrib(gen));
  }
}

int main() {
  std::filesystem::path path{"fstream-bench"};
  std::fstream fs;
  auto data = std::make_unique<char[]>(DATA_SIZE);
  RandomData(data.get(), DATA_SIZE);
  {
    // write
    std::cerr << "start fstream squence read write...\n";
    auto start = std::chrono::steady_clock::now();
    fs.open(path,
            std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
    auto end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "open file spend time:{} s\n",
        std::chrono::duration<double>(end - start).count());
    start = std::chrono::steady_clock::now();
    fs.write(data.get(), DATA_SIZE);
    end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "write speed: {} MB/s spend time: {} s\n",
        DATA_SIZE / 1024.0 / 1024 /
            std::chrono::duration<double>(end - start).count(),
        std::chrono::duration<double>(end - start).count());
    fs.flush();
    fs.seekg(0);
    // read
    auto buffer = std::make_unique<char[]>(DATA_SIZE);
    start = std::chrono::steady_clock::now();
    fs.read(buffer.get(), DATA_SIZE);
    end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "read speed: {} MB/s spend time: {} s\n",
        DATA_SIZE / 1024.0 / 1024 /
            std::chrono::duration<double>(end - start).count(),
        std::chrono::duration<double>(end - start).count());
    for (size_t i = 0; i < DATA_SIZE; i++) {
      if (buffer[i] != data[i]) {
        std::cerr << std::format("Read Error Data Real: {}, Expect: {}\n",
                                 buffer[i], data[i]);
        exit(1);
      }
    }
    // random write
    fs.seekp(0);

    // random read
    fs.seekg(0);

    fs.close();
    std::filesystem::remove(path);
  }
  std::cerr << '\n';
  {
    std::cerr << "start FILE squence read write...\n";
    std::FILE *file;
    auto start = std::chrono::steady_clock::now();
    file = fopen(path.c_str(), "w+");
    auto end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "open file spend time:{} s\n",
        std::chrono::duration<double>(end - start).count());
    start = std::chrono::steady_clock::now();
    fwrite(data.get(), 1, DATA_SIZE, file);
    end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "write speed: {} MB/s spend time: {} s\n",
        DATA_SIZE / 1024.0 / 1024 /
            std::chrono::duration<double>(end - start).count(),
        std::chrono::duration<double>(end - start).count());
    fflush(file);
    fseek(file, 0, SEEK_SET);
    // read
    auto buffer = std::make_unique<char[]>(DATA_SIZE);
    start = std::chrono::steady_clock::now();
    fread(buffer.get(), 1, DATA_SIZE, file);
    end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "read speed: {} MB/s spend time: {} s\n",
        DATA_SIZE / 1024.0 / 1024 /
            std::chrono::duration<double>(end - start).count(),
        std::chrono::duration<double>(end - start).count());
    for (size_t i = 0; i < DATA_SIZE; i++) {
      if (buffer[i] != data[i]) {
        std::cerr << std::format("Read Error Data Real: {}, Expect: {}\n",
                                 buffer[i], data[i]);
        exit(1);
      }
    }

    fclose(file);
    std::filesystem::remove(path);
  }
  std::cerr << '\n';
  {
    std::cerr << "start syscall sequence read write...\n";
    auto start = std::chrono::steady_clock::now();
    auto fd = open(path.c_str(), O_RDWR | O_CREAT);
    auto end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "open file spend time:{} s\n",
        std::chrono::duration<double>(end - start).count());
    start = std::chrono::steady_clock::now();
    auto t = write(fd, data.get(), DATA_SIZE);
    end = std::chrono::steady_clock::now();
    if (t == -1) {
      std::cerr << "write error\n";
    }
    std::cerr << std::format(
        "write speed: {} MB/s spend time: {} s\n",
        DATA_SIZE / 1024.0 / 1024 /
            std::chrono::duration<double>(end - start).count(),
        std::chrono::duration<double>(end - start).count());
    lseek(fd, 0, SEEK_SET);
    // read
    auto buffer = std::make_unique<char[]>(DATA_SIZE);
    start = std::chrono::steady_clock::now();
    read(fd, buffer.get(), DATA_SIZE);
    end = std::chrono::steady_clock::now();
    std::cerr << std::format(
        "read speed: {} MB/s spend time: {} s\n",
        DATA_SIZE / 1024.0 / 1024 /
            std::chrono::duration<double>(end - start).count(),
        std::chrono::duration<double>(end - start).count());
    for (size_t i = 0; i < DATA_SIZE; i++) {
      if (buffer[i] != data[i]) {
        std::cerr << std::format("Read Error Data Real: {}, Expect: {}\n",
                                 buffer[i], data[i]);
        exit(1);
      }
    }
    close(fd);
    std::filesystem::remove(path);
  }
  return 0;
}