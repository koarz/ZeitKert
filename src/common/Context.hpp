#pragma once

#include "buffer/BufferPoolManager.hpp"
#include "common/Config.hpp"
#include "common/DatabaseInstance.hpp"
#include "parser/SQLStatement.hpp"
#include "storage/disk/DiskManager.hpp"

#include <memory>
#include <string>
#include <unordered_map>

namespace DB {

class LSMTree;

struct QueryContext {
  std::shared_ptr<Database> database_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<SQLStatement> sql_statement_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unordered_map<std::string, std::shared_ptr<LSMTree>> lsm_trees_;

  QueryContext()
      : database_(nullptr), disk_manager_(std::make_shared<DiskManager>()),
        sql_statement_(nullptr),
        buffer_pool_manager_(std::make_shared<BufferPoolManager>(
            DEFAULT_POOL_SIZE, disk_manager_)) {}

  std::shared_ptr<LSMTree> GetOrCreateLSMTree(const TableMetaRef &table_meta);
};
} // namespace DB
