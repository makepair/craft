#pragma once

#include <stdint.h>
#include <map>
#include <string>

#include "raft.pb.h"

#include "mcast/Service.h"
#include "mcast/System.h"

#include "StateMachine.h"

namespace craft {

class RaftServer;
class RaftService;

class StateMachineService : public mcast::MethodCallService {
 public:
  typedef std::shared_ptr<LogEntry> LogEntryPtr;

  explicit StateMachineService(mcast::System *sys, const std::string &name,
                               const mcast::ServiceHandle &raft_service_h,
                               const std::string &server_id, const StateMachinePtr &sm)
      : mcast::MethodCallService(sys, name),
        raft_service_h_(raft_service_h),
        server_id_(server_id),
        state_machine_(sm) {}

  void StartStateMachine(int64_t term);

  void OnLogCommitted(int64_t term, int64_t committed_index);

 private:
  void OnServiceStop() override;
  void StopStateMachine();

  void Apply();
  bool Apply(const std::vector<LogEntryPtr> &entries);

  mcast::ServiceHandle raft_service_h_;
  int64_t current_term_{0};

  int64_t committed_index_{0};
  int64_t applied_index_{0};

  const std::string server_id_;
  StateMachinePtr state_machine_;
};

}  // namespace craft
