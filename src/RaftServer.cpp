#include "RaftServer.h"

#include "mcast/util/Logging.h"

#include "ProxyService.h"
#include "RaftService.h"
#include "StateMachineService.h"

namespace craft {

bool RaftServer::Start(const Configuration& config, const std::string& log_folder,
                       const StateMachinePtr& sm, unsigned int election_timeout_ms,
                       unsigned int append_entries_heartbeat_period_ms) {
  const std::string host_port_str = config.this_server_id();
  LOG_INFO << "RaftServer::Start " << host_port_str;

  system()->Start();
  raft_service_h_ = system()->LaunchService<RaftService>("RaftService");
  if (!raft_service_h_)
    return false;

  bool success = false;
  if (!system()->CallMethod(raft_service_h_, &RaftService::Start, config, sm, log_folder,
                            election_timeout_ms, append_entries_heartbeat_period_ms,
                            &success) ||
      !success) {
    Stop();
    return false;
  }

  auto raft_proxy_handle = rpc_server_.AddService(
      mcast::RpcServicePtr(new ProxyService(&sys_, raft_service_h_)));

  if (!raft_proxy_handle) {
    Stop();
    return false;
  }

  return rpc_server_.Start(host_port_str);
}

void RaftServer::Stop() {
  LOG_INFO << "RaftServer::Stop";
  system()->CallMethod(raft_service_h_, &RaftService::Stop);
  rpc_server_.Stop();
  system()->Stop();
}

void RaftServer::Wait() {
  system()->WaitStop();
}

void RaftServer::AddRpcService(const mcast::RpcServicePtr& s) {
  rpc_server_.AddService(s);
}

mcast::Status RaftServer::IsLeaderHint(bool* isleader) {
  return system()->CallMethod(raft_service_h_, &RaftService::IsLeader2, isleader);
}

mcast::Status RaftServer::GetLeaderHint(std::string* server_id) {
  return system()->CallMethod(raft_service_h_, &RaftService::GetLeader, server_id);
}

mcast::Status RaftServer::ExecuteCommand(const ::google::protobuf::Message* msg,
                                         Result* result) {
  std::string request;
  if (msg->SerializeToString(&request)) {
    return this->ExecuteCommand(request, result);
  }

  return mcast::Status(mcast::kFailed, "Message SerializeToString failed");
}

mcast::Status RaftServer::ExecuteCommand(const std::string& request, Result* result) {
  return system()->CallMethodWithClosure(raft_service_h_, &RaftService::ExecuteCommand,
                                         request, result);
}

mcast::Status RaftServer::WaitRead(Result* result) {
  return system()->CallMethodWithClosure(raft_service_h_, &RaftService::WaitRead, result);
}

mcast::Status RaftServer::AsyncExecuteCommand(const ::google::protobuf::Message* msg,
                                              Result* result, const Closure& closure) {
  std::string request;
  if (msg->SerializeToString(&request)) {
    return this->AsyncExecuteCommand(request, result, closure);
  }

  return mcast::Status(mcast::kFailed, "Message SerializeToString failed");
}

mcast::Status RaftServer::AsyncExecuteCommand(const std::string& request, Result* result,
                                              const Closure& closure) {
  return system()->AsyncCallMethod(raft_service_h_, &RaftService::ExecuteCommand, request,
                                   result, closure);
}

mcast::Status RaftServer::AsyncWaitRead(Result* result, const Closure& closure) {
  return system()->AsyncCallMethod(raft_service_h_, &RaftService::WaitRead, result,
                                   closure);
}

mcast::Status RaftServer::Lock(const LockRequest& lock_req, Result* result) {
  return system()->CallMethodWithClosure(raft_service_h_, &RaftService::Lock, lock_req,
                                         result);
}
mcast::Status RaftServer::Unlock(const UnlockRequest& unlock_req, Result* result) {
  return system()->CallMethodWithClosure(raft_service_h_, &RaftService::Unlock,
                                         unlock_req, result);
}

}  // namespace craft
