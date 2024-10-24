#pragma once

#include "simdjson.h"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/Int.hpp"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <sys/types.h>
#include <vector>

namespace DB {
class TableMeta {
  static constexpr std::string default_table_meta_name = "table_meta.json";

  std::string table_name_;
  std::vector<std::shared_ptr<ColumnWithNameType>> columns_;
  // primary_key
  std::map<std::string, uint> name_map_column_idx_;

public:
  explicit TableMeta(std::filesystem::path table_path) {
    std::ifstream fs{table_path / default_table_meta_name};
    std::string meta_data((std::istreambuf_iterator<char>(fs)),
                          std::istreambuf_iterator<char>());
    fs.close();
    simdjson::dom::parser parser;
    auto json = parser.parse(meta_data);
    table_name_ = json["table_name"];

    uint idx{};
    for (const auto &column : json["columns"]) {
      auto name = column["name"].get_string().value();
      auto type = column["type"].get_string().value();
      // auto nullable = column["nullable"].get_bool();
      if (type == "int") {
        columns_.push_back(std::make_shared<ColumnWithNameType>(
            std::make_shared<ColumnVector<int>>(), std::string(name),
            std::make_shared<Int>()));
        name_map_column_idx_.emplace(name, idx++);
      }
    }
  }

  explicit TableMeta(std::string &table_name,
                     std::vector<std::shared_ptr<ColumnWithNameType>> &columns)
      : table_name_(std::move(table_name)), columns_(std::move(columns)) {}

  std::string Serialize() {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    writer.StartObject();
    writer.Key("table_name");
    writer.String(table_name_.c_str());

    writer.Key("columns");
    writer.StartArray();

    for (const auto &column : columns_) {
      writer.StartObject();
      writer.Key("name");
      writer.String(column->name_.c_str());
      writer.Key("type");
      writer.String(column->type_->GetString().c_str());
      writer.EndObject();
    }

    writer.EndArray();
    writer.EndObject();

    return buffer.GetString();
  }

  std::shared_ptr<ColumnWithNameType> GetColumn(std::string &col_name) {
    return columns_[name_map_column_idx_[col_name]];
  }
};
} // namespace DB