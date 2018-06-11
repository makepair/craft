#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "raft.pb.h"

#include "mcast/System.h"

namespace craft {

class RaftService;

class ProxyService : public craft::RpcRaftService {
 public:
  ProxyService(mcast::System *sys, const mcast::ServiceHandle &h)
      : sys_(sys), raft_service_handle_(h) {}

  void RequestVote(::google::protobuf::RpcController *controller,
                   const ::craft::RequestVoteRequest *request,
                   ::craft::RequestVoteResponse *response,
                   ::google::protobuf::Closure *done) override;

  void AppendEntries(::google::protobuf::RpcController *controller,
                     const ::craft::AppendEntriesRequest *request,
                     ::craft::AppendEntriesResponse *response,
                     ::google::protobuf::Closure *done) override;

 private:
  mcast::System *sys_{nullptr};
  mcast::ServiceHandle raft_service_handle_;
};

}  // namespace craft
