#pragma once

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "raft.pb.h"

#include "mcast/util/Noncopyable.h"

namespace craft {

typedef std::shared_ptr<LogEntry> LogEntryPtr;

class Log : mcast::Noncopyable {
 public:
  ~Log() {
    Close();
  }

  bool Init(const std::string& data_folder);
  void Close();

  bool LoadMetaData(int64_t* current_term, std::string* voted_for);
  bool StoreMetaData(int64_t current_term, const std::string& voted_for);
  bool SyncMetaData();

  const LogEntryPtr& GetLogEntry(int64_t log_index);
  bool AppendLogEntry(const LogEntry& e);
  bool PopBackLogEntries(size_t num);
  bool Sync();

  const LogEntryPtr& LastLogEntry() {
    assert(!log_.empty());
    return log_.back();
  }

  int64_t LastLogTerm() {
    if (log_.empty()) {
      return 0;
    }
    return LastLogEntry()->term();
  }

  // log index starts from 1
  int64_t LastLogIndex() {
    return static_cast<int64_t>(log_.size());
  }

  size_t NumOfLogEntries() {
    return static_cast<size_t>(log_.size());
  }

 private:
  bool LoadLogEntries(std::vector<LogEntryPtr>* entries);

  struct __attribute__((packed)) MetaData {
    int64_t term;

    // 111.111.111.111:65535\n
    char server_id[32];  // change to  int64_t ?;
  };

  enum class FileLogEntryType { kEntry, kDeleteBack };

  struct __attribute__((packed)) FileLogEntry {
    uint32_t crc32;
    uint32_t size;
    int64_t term;
    uint8_t type;
    // follow by log data
  };

  std::vector<LogEntryPtr> log_;

  int meta_data_fd_ = -1;
  int log_data_fd_ = -1;

  MetaData* meta_data_ptr_ = nullptr;
  std::string data_folder_;
};

}  // namespace cratf
