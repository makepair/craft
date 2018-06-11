#pragma once

#include <stdint.h>

#include "raft.pb.h"

#include "mcast/RpcChannel.h"
#include "mcast/Service.h"
#include "mcast/System.h"

namespace craft {

class RaftService;

class PeerService : public mcast::UserThreadService {
 public:
  typedef std::shared_ptr<LogEntry> LogEntryPtr;

  PeerService(mcast::System *sys, const std::string &name,
              const mcast::ServiceHandle &raft_service_h, const std::string &server_id,
              const std::string &peer_id);

  void Main() override;

 private:
  void RequestVote(int64_t term, int64_t last_log_index, int64_t last_log_term);
  void AppendEntries(int64_t current_term, int64_t current_epoch,
                     int64_t committed_index);
  bool RPCAppendEntriesToPeer(int64_t term, int64_t committed_index,
                              const std::vector<LogEntryPtr> &entries,
                              ::craft::AppendEntriesRequest *request,
                              ::craft::AppendEntriesResponse *response);

  void OnServiceStart() override;
  void OnServiceStop() override;

  int64_t PrevLogIndex() const {
    CHECK_GT(next_log_, 0);
    return next_log_ - 1;
  }

  int64_t PrevLogTerm() const {
    return prev_log_term_;
  }

  mcast::ServiceHandle raft_service_h_;

  int64_t next_log_{1};
  int64_t prev_log_term_{0};

  const std::string server_id_;
  const std::string peer_id_;

  mcast::RpcChannel rcp_channel_;
};

}  // namespace craft
