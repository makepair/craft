#include "ProxyService.h"

#include "RaftService.h"

namespace craft {

void ProxyService::RequestVote(::google::protobuf::RpcController *controller,
                               const ::craft::RequestVoteRequest *request,
                               ::craft::RequestVoteResponse *response,
                               ::google::protobuf::Closure *done) {
  auto s = sys_->CallMethod(raft_service_handle_, &RaftService::HandleRequestVote,
                            request, response);
  if (!s) {
    controller->SetFailed(s.ErrorText());
  }
  done->Run();
}

void ProxyService::AppendEntries(::google::protobuf::RpcController *controller,
                                 const ::craft::AppendEntriesRequest *request,
                                 ::craft::AppendEntriesResponse *response,
                                 ::google::protobuf::Closure *done) {
  auto s = sys_->CallMethod(raft_service_handle_, &RaftService::HandleAppendEntries,
                            request, response);
  if (!s) {
    controller->SetFailed(s.ErrorText());
  }
  done->Run();
}

}  // namespace craft
