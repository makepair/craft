#include "Log.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>

#include "mcast/util/Logging.h"
#include "mcast/util/util.h"

namespace craft {

// static bool FileSize(const std::string& fname, uint64_t* size) {
//  struct stat sbuf;
//  if (stat(fname.c_str(), &sbuf) != 0) {
//    *size = 0;
//    return false;
//  } else {
//    *size = static_cast<uint64_t>(sbuf.st_size);
//    return true;
//  }
//}

// static bool Read(int fd, void* vbuf, size_t n) {
//  assert(fd >= 0);
//  uint8_t* buf = static_cast<uint8_t*>(vbuf);
//  size_t left = n;
//
//  while (left != 0) {
//    ssize_t r;
//    if ((r = read(fd, buf + (n - left), left)) > 0) {
//      left -= static_cast<size_t>(r);
//    } else if (r == 0) {
//      return false;
//    } else if (errno != EINTR)
//      return false;
//  }
//
//  return true;
//}
//

static bool Write(int fd, const void* vbuf, size_t n) {
  assert(fd >= 0);
  const uint8_t* buf = static_cast<const uint8_t*>(vbuf);
  size_t left = n;
  while (left != 0) {
    ssize_t r;
    if ((r = write(fd, buf + (n - left), left)) >= 0) {
      left -= static_cast<size_t>(r);
    } else if (errno != EINTR)
      return false;
  }

  return true;
}

static bool FileSize(const int fd, uint64_t* size) {
  struct stat sbuf;
  if (fstat(fd, &sbuf) != 0) {
    *size = 0;
    return false;
  } else {
    *size = static_cast<uint64_t>(sbuf.st_size);
    return true;
  }
}

bool Log::Init(const std::string& data_folder) {
  LOG_INFO << "Log folder " << data_folder;

  Close();
  data_folder_ = data_folder;
  meta_data_fd_ =
      open((data_folder + "/meta.dat").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (meta_data_fd_ < 0) {
    Close();
    return false;
  }

  uint64_t meta_file_size = 0;
  if (!FileSize(meta_data_fd_, &meta_file_size)) {
    LOG_WARN << "get meta file size error:" << ERRNO_TEXT;
    return false;
  }

  if (meta_file_size == 0) {
    LOG_INFO << "meta file empty";
    if (0 != ftruncate(meta_data_fd_, sizeof(MetaData))) {
      LOG_WARN << "meta file truncate error:" << ERRNO_TEXT;
      return false;
    }
    meta_file_size = sizeof(MetaData);
  }

  if (meta_file_size != sizeof(MetaData)) {
    LOG_WARN << "MetaData file corruption";
    return false;
  }

  uint8_t* p = static_cast<uint8_t*>(
      mmap(NULL, meta_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, meta_data_fd_, 0));
  if (p == MAP_FAILED) {
    LOG_WARN << "meta file mmap error:" << ERRNO_TEXT;
    return false;
  }
  meta_data_ptr_ = reinterpret_cast<MetaData*>(p);

  log_data_fd_ = open((data_folder + "/log_data.dat").c_str(),
                      O_RDWR | O_CREAT | O_CLOEXEC | O_APPEND, 0644);
  if (log_data_fd_ < 0) {
    Close();
    return false;
  }

  if (!LoadLogEntries(&log_)) {
    LOG_WARN << "LoadLogEntries error";
  }

  return true;
}

void Log::Close() {
  if (meta_data_fd_ != -1) {
    close(meta_data_fd_);
  }
  if (log_data_fd_ != -1) {
    close(log_data_fd_);
  }

  if (meta_data_ptr_) {
    munmap(meta_data_ptr_, sizeof(MetaData));
  }

  meta_data_fd_ = -1;
  log_data_fd_ = -1;
  meta_data_ptr_ = nullptr;
  log_.clear();
}

bool Log::LoadMetaData(int64_t* current_term, std::string* voted_for) {
  *current_term = meta_data_ptr_->term;
  voted_for->assign(meta_data_ptr_->server_id);

  LOG_INFO << "LoadMetaData: term " << *current_term << ",voted for " << *voted_for;

  return true;
}

bool Log::StoreMetaData(int64_t current_term, const std::string& voted_for) {
  meta_data_ptr_->term = current_term;
  voted_for.copy(meta_data_ptr_->server_id, voted_for.size());
  meta_data_ptr_->server_id[voted_for.size()] = '\0';

  return true;
}

bool Log::SyncMetaData() {
  return 0 == fdatasync(meta_data_fd_);
}

bool Log::LoadLogEntries(std::vector<LogEntryPtr>* entries) {
  uint64_t file_size = 0;
  if (!FileSize(log_data_fd_, &file_size)) {
    LOG_WARN << "LoadLogData get files size error:" << ERRNO_TEXT;
    return false;
  }

  if (0 == file_size) {
    LOG_WARN << "LoadLogData log file empty";
    return true;
  }

  LOG_INFO << "LoadLogData log file size " << file_size;

  uint8_t* p = static_cast<uint8_t*>(
      mmap(NULL, file_size, PROT_READ, MAP_SHARED, log_data_fd_, 0));
  if (p == MAP_FAILED) {
    LOG_WARN << "mmap error:" << ERRNO_TEXT;
    return false;
  }

  size_t offset = 0;
  while (offset < file_size) {
    FileLogEntry* pe = reinterpret_cast<FileLogEntry*>(p + offset);
    if (FileLogEntryType(pe->type) == FileLogEntryType::kEntry) {
      LogEntryPtr log_entry(LogEntry::default_instance().New());
      if (!log_entry->ParseFromArray(reinterpret_cast<char*>(pe + 1),
                                     static_cast<int>(pe->size))) {
        LOG_WARN << "LogEntry ParseFromArray Failed";
        return false;
      }
      entries->push_back(std::move(log_entry));
      offset += sizeof(FileLogEntry) + pe->size;
    } else if (FileLogEntryType(pe->type) == FileLogEntryType::kDeleteBack) {
      CHECK(entries->size() >= pe->size);
      for (uint32_t i = 0; i < pe->size; ++i) {
        entries->pop_back();
      }

      offset += sizeof(FileLogEntry);
    } else {
      LOG_WARN << "LoadLogData: file corruption";
      entries->clear();
      return false;
    }
  }

  munmap(p, file_size);
  CHECK(offset == file_size);
  LOG_INFO << "LoadLogData: number of entries " << entries->size();
  return true;
}

const LogEntryPtr& Log::GetLogEntry(int64_t log_index) {
  CHECK_GT(log_index, 0);
  CHECK_LE(log_index, LastLogIndex());

  return log_[static_cast<size_t>(log_index - 1)];
}

bool Log::AppendLogEntry(const LogEntry& e) {
  LogEntryPtr le(LogEntry::default_instance().New());
  *le = e;
  log_.push_back(le);

  std::string buf;
  buf.resize(e.ByteSizeLong() + sizeof(FileLogEntry));

  FileLogEntry lde;
  lde.crc32 = 0;
  lde.term = e.term();
  lde.type = static_cast<uint8_t>(FileLogEntryType::kEntry);
  lde.size = static_cast<uint32_t>(e.GetCachedSize());
  *reinterpret_cast<FileLogEntry*>(&buf[0]) = lde;

  if (e.SerializeToArray(&buf[sizeof(lde)], static_cast<int>(lde.size))) {
    return Write(log_data_fd_, buf.data(), buf.size());
  } else {
    LOG_WARN << "LogEntry SerializeToArray Failed";
    return false;
  }
}

bool Log::PopBackLogEntries(size_t num) {
  CHECK(num <= std::numeric_limits<decltype(FileLogEntry::size)>::max());

  size_t new_size = log_.size() - num;
  log_.resize(new_size);

  union {
    FileLogEntry lde;
    char buf[sizeof(FileLogEntry)];
  };

  lde.crc32 = 0;
  lde.term = 0;
  lde.type = static_cast<uint8_t>(FileLogEntryType::kDeleteBack);
  lde.size = static_cast<uint32_t>(num);
  return Write(log_data_fd_, buf, sizeof(FileLogEntry));
}

bool Log::Sync() {
  return 0 == fdatasync(log_data_fd_);
}

}  // namespace craft
