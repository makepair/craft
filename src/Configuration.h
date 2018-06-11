#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace craft {

struct ServerInfo {
  std::string server_id;
  bool granted_vote = false;
  int64_t match_log = 0;
  int64_t epoch = 0;
};

class Configuration {
 public:
  void SetThisServerId(const std::string &sid) {
    this_server_id_ = sid;
  }

  void AddServerId(const std::string &sid) {
    ServerInfo sinfo;
    sinfo.server_id = sid;
    server_ids_.emplace(sid, sinfo);
  }

  void StepDown() {
    for (auto &p : server_ids_) {
      p.second.granted_vote = false;
      p.second.match_log = 0;
      p.second.epoch = 0;
    }
  }

  void GrantedVote(const std::string &sid) {
    server_ids_[sid].granted_vote = true;
  }

  void UpdateMatchLog(const std::string &sid, int64_t match_log) {
    server_ids_[sid].match_log = match_log;
  }

  void UpdateEpoch(const std::string &sid, int64_t epoch) {
    server_ids_[sid].epoch = epoch;
  }

  ServerInfo &GetServerInfo(const std::string &sid) {
    return server_ids_[sid];
  }

  bool QuorumAll(const std::function<bool(const ServerInfo &)> &predicate) {
    if (server_ids_.empty())
      return true;

    size_t num = 0;
    size_t const min_num = server_ids_.size() / 2 + 1;

    for (auto &p : server_ids_) {
      if (predicate(p.second))
        ++num;
    }

    return num >= min_num;
  }

  int64_t QuorumMin(const std::function<int64_t(const ServerInfo &)> &GetValue) {
    if (server_ids_.empty())
      return 0;

    size_t const min_index = (server_ids_.size() - 1) / 2;

    std::vector<int64_t> values;
    for (auto &p : server_ids_) {
      values.push_back(GetValue(p.second));
    }

    std::sort(values.begin(), values.end());
    return values.at(min_index);
  }

  template <typename Func>
  void ForeachServer(Func &&fnc, bool including_this_server = false) {
    for (auto &p : server_ids_) {
      if (including_this_server)
        fnc(p.first);
      else if (this_server_id_ != p.first)
        fnc(p.first);
    }
  }

  template <typename Func>
  void Foreach(Func &&fnc, bool including_this_server = false) {
    for (auto &p : server_ids_) {
      if (including_this_server)
        fnc(p.second);
      else if (this_server_id_ != p.first)
        fnc(p.second);
    }
  }

  const std::string &this_server_id() const {
    return this_server_id_;
  }

  size_t ServerNum() const {
    return server_ids_.size();
  }

 private:
  std::string this_server_id_;
  std::unordered_map<std::string, ServerInfo> server_ids_;
};

}  // namespace craft
