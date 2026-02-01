#include "common/Logger.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "common/ZeitKert.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace DB {

enum class StatementType { StatementOk, StatementError, Query };

struct TestCase {
  StatementType type;
  std::string sql;
  std::vector<std::string> expected_output;
};

class SQLTestRunner {
public:
  SQLTestRunner() : db_(std::make_unique<ZeitKert>()) {}

  bool RunTestFile(const std::string &file_path) {
    std::cout << "Running test file: " << file_path << "\n";
    std::ifstream file(file_path);
    if (!file.is_open()) {
      std::cerr << "Failed to open file: " << file_path << "\n";
      return false;
    }

    auto test_cases = ParseTestFile(file);
    file.close();

    bool all_passed = true;
    int test_num = 0;
    for (const auto &test : test_cases) {
      test_num++;
      if (!RunTestCase(test, test_num)) {
        all_passed = false;
      }
    }

    std::cout << (all_passed ? "✓ All tests passed\n" : "✗ Some tests failed\n")
              << "\n";
    return all_passed;
  }

private:
  std::vector<TestCase> ParseTestFile(std::ifstream &file) {
    std::vector<TestCase> cases;
    std::string line;
    TestCase current_test;
    bool in_result = false;

    while (std::getline(file, line)) {
      // 跳过空行和注释
      if (line.empty() || line[0] == '#') {
        continue;
      }

      // 处理结果分隔符
      if (line == "----") {
        in_result = true;
        continue;
      }

      // 解析测试类型
      if (line.find("statement ok") == 0) {
        if (!current_test.sql.empty()) {
          cases.push_back(current_test);
        }
        current_test = TestCase{StatementType::StatementOk, "", {}};
        in_result = false;
        continue;
      }

      if (line.find("statement error") == 0) {
        if (!current_test.sql.empty()) {
          cases.push_back(current_test);
        }
        current_test = TestCase{StatementType::StatementError, "", {}};
        in_result = false;
        continue;
      }

      if (line.find("query") == 0) {
        if (!current_test.sql.empty()) {
          cases.push_back(current_test);
        }
        current_test = TestCase{StatementType::Query, "", {}};
        in_result = false;
        continue;
      }

      // 收集SQL或预期结果
      if (in_result) {
        current_test.expected_output.push_back(line);
      } else {
        if (!current_test.sql.empty()) {
          current_test.sql += " ";
        }
        current_test.sql += line;
      }
    }

    if (!current_test.sql.empty()) {
      cases.push_back(current_test);
    }

    return cases;
  }

  bool RunTestCase(const TestCase &test, int test_num) {
    std::string sql = test.sql;
    if (sql.back() != ';') {
      sql += ";";
    }

    ResultSet result;
    Status status = db_->ExecuteQuery(sql, result);

    switch (test.type) {
    case StatementType::StatementOk:
      if (!status.ok()) {
        std::cerr << "Test #" << test_num << " FAILED\n";
        std::cerr << "  SQL: " << test.sql << "\n";
        std::cerr << "  Expected: OK\n";
        std::cerr << "  Got: " << status.GetMessage() << "\n";
        return false;
      }
      std::cout << "Test #" << test_num << " passed (statement ok)\n";
      return true;

    case StatementType::StatementError:
      if (status.ok()) {
        std::cerr << "Test #" << test_num << " FAILED\n";
        std::cerr << "  SQL: " << test.sql << "\n";
        std::cerr << "  Expected: ERROR\n";
        std::cerr << "  Got: OK\n";
        return false;
      }
      std::cout << "Test #" << test_num << " passed (statement error)\n";
      return true;

    case StatementType::Query:
      if (!status.ok()) {
        std::cerr << "Test #" << test_num << " FAILED\n";
        std::cerr << "  SQL: " << test.sql << "\n";
        std::cerr << "  Expected: OK with results\n";
        std::cerr << "  Got: " << status.GetMessage() << "\n";
        return false;
      }

      // 验证结果
      if (!result.schema_) {
        std::cerr << "Test #" << test_num << " FAILED\n";
        std::cerr << "  SQL: " << test.sql << "\n";
        std::cerr << "  Expected results but got none\n";
        return false;
      }

      auto actual_output = GetQueryOutput(result);
      if (actual_output != test.expected_output) {
        std::cerr << "Test #" << test_num << " FAILED\n";
        std::cerr << "  SQL: " << test.sql << "\n";
        std::cerr << "  Expected:\n";
        for (const auto &line : test.expected_output) {
          std::cerr << "    " << line << "\n";
        }
        std::cerr << "  Got:\n";
        for (const auto &line : actual_output) {
          std::cerr << "    " << line << "\n";
        }
        return false;
      }

      std::cout << "Test #" << test_num << " passed (query)\n";
      return true;
    }

    return false;
  }

  std::vector<std::string> GetQueryOutput(const ResultSet &result) {
    std::vector<std::string> output;
    if (!result.schema_) {
      return output;
    }

    auto &columns = result.schema_->GetColumns();
    if (columns.empty()) {
      return output;
    }

    size_t row_count = columns[0]->Size();
    for (size_t i = 0; i < row_count; i++) {
      std::string row;
      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0) {
          row += " ";
        }
        row += columns[j]->GetStrElement(i);
      }
      output.push_back(row);
    }

    return output;
  }

  std::unique_ptr<ZeitKert> db_;
};

} // namespace DB

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: sqltest <test_file.sql> [test_file2.sql ...]\n";
    std::cerr << "       sqltest a/b/*.sql\n";
    return 1;
  }

  DB::Logger::Init("./logs/sqltest.log");

  DB::SQLTestRunner runner;
  bool all_passed = true;

  for (int i = 1; i < argc; i++) {
    std::string pattern = argv[i];

    // 检查是否包含通配符
    if (pattern.find('*') != std::string::npos ||
        pattern.find('?') != std::string::npos) {
      // 分离目录和文件名模式
      std::filesystem::path p(pattern);
      std::string dir = p.parent_path().string();
      if (dir.empty()) {
        dir = ".";
      }
      std::string file_pattern = p.filename().string();

      // 遍历目录查找匹配文件
      for (const auto &entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file()) {
          std::string filename = entry.path().filename().string();
          // 简单通配符匹配（只支持 *.sql）
          if (file_pattern == "*.sql" && filename.ends_with(".sql")) {
            if (!runner.RunTestFile(entry.path().string())) {
              all_passed = false;
            }
          } else if (filename == file_pattern) {
            if (!runner.RunTestFile(entry.path().string())) {
              all_passed = false;
            }
          }
        }
      }
    } else {
      // 直接运行单个文件
      if (!runner.RunTestFile(pattern)) {
        all_passed = false;
      }
    }
  }

  DB::Logger::Shutdown();

  std::cout << "\n"
            << (all_passed ? "=== All test files passed ==="
                           : "=== Some test files failed ===")
            << "\n";

  return all_passed ? 0 : 1;
}
