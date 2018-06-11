#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "mcast/util/Noncopyable.h"

class KVDB : public mcast::Noncopyable {
 public:
  static KVDB &Instance() {
    static KVDB kvdb;
    return kvdb;
  }

  bool Get(const std::string &key, std::string *value) {
    std::lock_guard<std::mutex> lg(mutex_);
    auto it = db_.find(key);
    if (it != db_.end()) {
      *value = it->second;
      return true;
    }

    return false;
  }

  bool Put(const std::string &key, const std::string &value) {
    std::lock_guard<std::mutex> lg(mutex_);
    db_[key] = value;
    return true;
  }

 private:
  KVDB() = default;
  std::mutex mutex_;
  std::unordered_map<std::string, std::string> db_;
};
