#pragma once

#include "raft.pb.h"

#include "mcast/RpcServer.h"
#include "mcast/ServiceHandle.h"
#include "mcast/System.h"

#include "Closure.h"
#include "Configuration.h"
#include "Result.h"
#include "StateMachine.h"

namespace craft {

class RaftService;

class RaftServer : public mcast::Noncopyable {
 public:
  RaftServer() : rpc_server_(&sys_) {}

  bool Start(const Configuration &config, const std::string &log_folder,
             const StateMachinePtr &sm, unsigned int election_timeout_ms = 500,
             unsigned int append_entries_heartbeat_period_ms = 250);
  void Stop();
  void Wait();

  mcast::Status ExecuteCommand(const ::google::protobuf::Message *msg, Result *result);
  mcast::Status ExecuteCommand(const std::string &request, Result *result);
  mcast::Status WaitRead(Result *result);

  mcast::Status AsyncExecuteCommand(const ::google::protobuf::Message *msg,
                                    Result *result, const Closure &closure);
  mcast::Status AsyncExecuteCommand(const std::string &request, Result *result,
                                    const Closure &closure);
  mcast::Status AsyncWaitRead(Result *result, const Closure &closure);

  mcast::Status IsLeaderHint(bool *isleader);
  mcast::Status GetLeaderHint(std::string *server_id);

  void AddRpcService(const mcast::RpcServicePtr &s);

  mcast::System *system() {
    return &sys_;
  }

  mcast::Status Lock(const LockRequest &lock_req, Result *result);
  mcast::Status Unlock(const UnlockRequest &unlock_req, Result *result);
  //  mcast::Status IsLock(const std::string &key, const Closure &closure);
 private:
  mcast::System sys_;
  mcast::RpcServer rpc_server_;
  mcast::ServiceHandle raft_service_h_;
};

}  // namespace craft
