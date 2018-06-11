#pragma once

#include <stdint.h>

#include <functional>
#include <map>
#include <string>
#include <vector>

#include "raft.pb.h"

#include "mcast/RpcChannel.h"
#include "mcast/ServiceHandle.h"
#include "mcast/System.h"
#include "mcast/TcpConnector.h"

#include "Closure.h"
#include "Configuration.h"
#include "Log.h"
#include "Result.h"
#include "StateMachine.h"

namespace craft {

class RaftService : public mcast::MethodCallService {
 public:
  enum State { kState_Invail = 0, kState_Follower, kState_Candidate, kState_Leader };

  explicit RaftService(mcast::System *sys, const std::string &name)
      : mcast::MethodCallService(sys, name), rand_(rand_dev_()) {}

  void Start(const Configuration &config, const StateMachinePtr &sm,
             const std::string &log_folder, unsigned int election_timeout_ms,
             unsigned int append_entries_heartbeat_period_ms, bool *success);
  void Stop();

  void ExecuteCommand(const std::string &request, Result *result, const Closure &closure);
  void WaitRead(Result *result, const Closure &closure);

  void Lock(const LockRequest &lock_req, Result *result, const Closure &closure);
  void Unlock(const UnlockRequest &unlock_req, Result *result, const Closure &closure);

  void HandleRequestVote(const ::craft::RequestVoteRequest *request,
                         ::craft::RequestVoteResponse *response);
  void HandleAppendEntries(const ::craft::AppendEntriesRequest *request,
                           ::craft::AppendEntriesResponse *response);

  void GetState(State *state, int64_t *current_term, int64_t *current_epoch,
                int64_t *committed_index, int64_t *last_log_index,
                int64_t *last_log_term);

  void StepDown(int64_t term);
  void GetLogEntries(int64_t log_index_start, int max_num,
                     std::vector<LogEntryPtr> *entries, int64_t *last_log_index);

  LogEntryPtr GetLogEntry(int64_t log_index) {
    LogEntryPtr log_entry;
    GetLogEntry(log_index, &log_entry);
    return log_entry;
  }
  void GetLogEntry(int64_t log_index, LogEntryPtr *log_entry);

  void OnRequestVoteResponse(const std::string &peer_id,
                             const ::craft::RequestVoteRequest &request,
                             const ::craft::RequestVoteResponse &response);
  void OnAppendEntriesResponse(const std::string &peer_id, int64_t epoch,
                               const ::craft::AppendEntriesRequest &request,
                               const ::craft::AppendEntriesResponse &response,
                               bool *avail);

  void OnLogExecuted(LogEntry::LogEntryType log_type, int64_t log_index,
                     const std::string &response);
  void OnCommandDone(int64_t log_index, Result::Type type, const std::string &response);
  void OnCommandDone(int64_t log_index, ::google::protobuf::Message *response);

  void GetLeader(std::string *serverid) {
    *serverid = leader_id_;
  }
  void IsLeader2(bool *isleader) {
    *isleader = IsLeader() ? true : false;
  }

  void OnLogSync(int64_t last_log_sync_index);

 private:
  void AdvanceCommittedIndex();

  bool IsLeader() const {
    return state_ == kState_Leader;
  }
  void OnServiceStop() override;

  void RestartElectionTimer();
  void StopElectionTimer();

  void StartElection();
  void BecomeLeader();

  void RestartStepDownTimer();
  void StopStepDownTimer();
  void CheckStepDown(int64_t epoch);

  bool CheckIsLeader(Result *reuslt, const Closure &closure);

  int64_t AppendLog(const std::string &log_data);
  int64_t AppendLog(const LogEntry &le);

  bool PersistMetaData();

  void RestartAppendEntriesTimer();
  void StopAppendEntriesTimer();
  void StartAppendEntries();

  void UpdateMatchLogIndex(int64_t log_index) {
    configuration_.UpdateMatchLog(server_id_, log_index);
  }

  void AdvanceEpoch();

  size_t NumOfLogEntries() {
    return log_.NumOfLogEntries();
  }

  int64_t LastLogTerm() {
    return log_.LastLogTerm();
  }

  // log index starts from 1
  int64_t LastLogIndex() {
    return log_.LastLogIndex();
  }

  const LogEntryPtr &LastLogEntry() {
    return log_.LastLogEntry();
  }

  int64_t current_term_{0};
  std::string voted_for_;

  Log log_;
  bool log_sync_{false};

  State state_{kState_Follower};
  int64_t committed_index_{0};
  int64_t applied_index_{0};
  std::string server_id_;
  std::string leader_id_;

  int64_t current_epoch_{0};
  Configuration configuration_;

  bool is_election_timer_start_{false};
  mcast::TimerHandle election_timer_h_;
  unsigned int election_timeout_ms_ = 500;

  bool is_stepdown_timer_start_{false};
  mcast::TimerHandle stepdown_timer_h_;

  StateMachinePtr state_machine_;
  mcast::ServiceHandle state_machine_h_;
  mcast::ServiceHandle leader_disk_h_;

  std::vector<mcast::ServiceHandle> peer_srv_handles_;

  std::map<int64_t, std::pair<Result *, Closure>> command_closures_;
  std::multimap<int64_t, std::pair<Result *, Closure>> readonly_command_closures_;

  typedef std::function<void(bool)> CallBack;
  std::map<int64_t, CallBack> leader_uptodate_callback_;

  std::random_device rand_dev_;
  std::minstd_rand rand_;

  bool is_append_entries_timer_start_{false};
  mcast::TimerHandle append_entries_timer_h_;
  uint32_t append_entries_heartbeat_period_ms_ = 200;

  std::unordered_map<std::string, std::pair<std::string, int64_t>> locks_;
};

}  // namespace craft
