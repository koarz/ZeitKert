#pragma once

#include "common/EnumClass.hpp"

#include <memory>
#include <string>

namespace DB {

class [[nodiscard]] Status {
public:
  Status() : code_(ErrorCode::OK) {}

  Status(Status &&rhs) noexcept = default;

  Status(const Status &rhs) { *this = rhs; }

  Status &operator=(const Status &rhs) {
    code_ = rhs.code_;
    if (rhs.err_msg_) {
      err_msg_ = std::make_unique<ErrMsg>(*rhs.err_msg_);
    }
    return *this;
  }

  Status &operator=(Status &&rhs) noexcept {
    code_ = rhs.code_;
    if (rhs.err_msg_) {
      err_msg_ = std::move(rhs.err_msg_);
    }
    return *this;
  }

  Status(ErrorCode code, std::string msg) : code_(code) {
    err_msg_ = std::make_unique<ErrMsg>();
    err_msg_->msg_ = std::move(msg);
  }

  Status static Error(ErrorCode code, std::string msg) {
    Status status;
    status.code_ = code;
    status.err_msg_ = std::make_unique<ErrMsg>();
    status.err_msg_->msg_ = std::move(msg);
    return status;
  }

  static Status OK() { return {}; }

  bool ok() const { return code_ == ErrorCode::OK; }

  std::string GetMessage() {
    if (code_ == ErrorCode::OK) {
      return "OK\n";
    }
    return err_msg_->msg_;
  }

private:
  ErrorCode code_;

  struct ErrMsg {
    std::string msg_;
  };

  std::unique_ptr<ErrMsg> err_msg_;
};
} // namespace DB