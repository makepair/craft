#include "RaftService.h"

#include <time.h>
#include <random>

#include "mcast/util/DateTime.h"
#include "mcast/util/Logging.h"
#include "mcast/util/Timer.h"

#include "PeerService.h"
#include "ProxyService.h"
#include "RaftServer.h"
#include "StateMachineService.h"

namespace craft {

class LeaderDiskSerivce : public mcast::UserThreadService {
 public:
  LeaderDiskSerivce(mcast::System* sys, const std::string& name,
                    const mcast::ServiceHandle& raft_service_h,
                    const std::function<void()>& log_sync)
      : UserThreadService(sys, name),
        raft_service_h_(raft_service_h),
        log_sync_(log_sync) {}

  void Main() override {
    LOG_INFO << "LeaderDiskSerivce running";

    int64_t current_term = 0;
    int64_t current_epoch = 0;
    int64_t committed_index = 0;
    int64_t last_log_term = 0;
    int64_t last_log_index = 0;
    int64_t last_log_index_sync = 0;
    RaftService::State state = RaftService::kState_Invail;

    while (!IsStopping()) {
      if (!system()->CallMethod(raft_service_h_, &RaftService::GetState, &state,
                                &current_term, &current_epoch, &committed_index,
                                &last_log_index, &last_log_term)) {
        break;
      }
      if (state == RaftService::kState_Leader && last_log_index > last_log_index_sync) {
        mcast::Timer timer;
        timer.Start();
        log_sync_();
        LOG_INFO << "LeaderDiskSerivce: log sync spent " << std::fixed
                 << timer.Elapsed().ToSeconds() << " seconds";

        last_log_index_sync = last_log_index;
        if (!system()->CallMethod(raft_service_h_, &RaftService::OnLogSync,
                                  last_log_index)) {
          break;
        }
      } else {
        WaitSignal();
      }
    }

    LOG_INFO << "LeaderDiskSerivce stop";
  }

 private:
  mcast::ServiceHandle raft_service_h_;
  std::function<void()> log_sync_;
};

void RaftService::Start(const Configuration& config, const StateMachinePtr& sm,
                        const std::string& log_folder, unsigned int election_timeout_ms,
                        unsigned int append_entries_heartbeat_period_ms, bool* success) {
  LOG_TRACE << "RaftService::Start, log folder " << log_folder << ",election_timeout_ms "
            << election_timeout_ms;

  *success = false;
  if (!log_.Init(log_folder)) {
    LOG_WARN << "Log init failed";
    return;
  }

  state_ = kState_Follower;
  server_id_ = config.this_server_id();
  configuration_ = config;
  state_machine_ = sm;

  election_timeout_ms_ = election_timeout_ms;
  append_entries_heartbeat_period_ms_ = append_entries_heartbeat_period_ms;

  voted_for_.clear();
  if (!log_.LoadMetaData(&current_term_, &voted_for_)) {
    LOG_WARN << "LoadMetaData failed";
    return;
  }

  if (sm) {
    state_machine_h_ = system()->LaunchService<StateMachineService>(
        "StateMachineService", handle(), server_id_, sm);
    if (!state_machine_h_) {
      LOG_WARN << "launch statemachine failed";
      return;
    }
    if (!system()->CallMethod(state_machine_h_, &StateMachineService::StartStateMachine,
                              current_term_)) {
      LOG_WARN << "start statemachine failed";
      return;
    }
  }

  // must after lauch StateMachineSerive
  CHECK(peer_srv_handles_.empty());
  bool launch_peer_service_ok = true;
  configuration_.ForeachServer([this,
                                &launch_peer_service_ok](const std::string& peerid) {
    auto h =
        system()->LaunchService<PeerService>("PeerService", handle(), server_id_, peerid);

    if (h) {
      LOG_INFO << "launch peer service peer id " << peerid;
      this->peer_srv_handles_.push_back(h);
    } else {
      LOG_WARN << "launch peer service failed, peer id " << peerid;
      launch_peer_service_ok = false;
    }
  });

  if (!launch_peer_service_ok)
    return;

  leader_disk_h_ = system()->LaunchService<LeaderDiskSerivce>(
      "LeaderDiskSerivce", handle(), [this]() mutable { log_.Sync(); });

  if (!leader_disk_h_) {
    LOG_WARN << "launch leader disk service  failed";
    return;
  }

  UpdateMatchLogIndex(LastLogIndex());
  AdvanceCommittedIndex();
  RestartElectionTimer();
  *success = true;
}

void RaftService::Stop() {
  LOG_INFO << "RaftService Stop";
  StopElectionTimer();

  StepDown(current_term_);
  system()->StopService(leader_disk_h_);

  for (auto& h : peer_srv_handles_) {
    system()->StopService(h);
  }

  if (state_machine_h_) {
    system()->StopService(state_machine_h_);
  }

  peer_srv_handles_.clear();
}

void RaftService::OnServiceStop() {
  LOG_INFO << "OnServiceStop";
  CHECK(peer_srv_handles_.empty());
}

void RaftService::StepDown(int64_t term) {
  LOG_INFO << "StepDown: current term " << current_term_ << ", new term " << term;

  CHECK_GE(term, current_term_);
  state_ = kState_Follower;
  if (log_sync_) {
    CHECK(log_.Sync());
    log_sync_ = false;

    UpdateMatchLogIndex(LastLogIndex());
    AdvanceCommittedIndex();
  }

  if (term > current_term_) {
    LOG_TRACE << "change current term " << current_term_ << " to " << term;
    current_term_ = term;
    voted_for_.clear();
    leader_id_.clear();
    CHECK(PersistMetaData());
  }

  configuration_.StepDown();
  for (auto& p : leader_uptodate_callback_) {
    p.second(false);
  }

  for (auto& p : readonly_command_closures_) {
    p.second.first->type = Result::kNotLeader;
    p.second.second();
  }
  readonly_command_closures_.clear();

  for (auto& p : command_closures_) {
    p.second.first->type = Result::kNotLeader;
    p.second.second();
  }
  command_closures_.clear();

  for (auto& h : peer_srv_handles_) {
    system()->InterruptService(h);
  }
}

void RaftService::GetState(State* state, int64_t* current_term, int64_t* current_epoch,
                           int64_t* committed_index, int64_t* last_log_index,
                           int64_t* last_log_term) {
  *state = state_;
  *current_term = current_term_;
  *current_epoch = current_epoch_;
  *committed_index = committed_index_;
  *last_log_index = LastLogIndex();
  *last_log_term = LastLogTerm();
}

void RaftService::RestartElectionTimer() {
  StopElectionTimer();

  if (IsLeader())
    return;

  typedef std::uniform_int_distribution<uint32_t> distr_t;
  typedef typename distr_t::param_type param_t;
  distr_t D;

  uint32_t timeout = election_timeout_ms_ + D(rand_, param_t(0, election_timeout_ms_));
  LOG_TRACE << "RestartElectionTimer " << timeout << " ms";

  is_election_timer_start_ = true;
  auto sys = this->system();
  auto h = handle();
  election_timer_h_ = system()->AddTimer(
      timeout, [sys, h]() { sys->AsyncCallMethod(h, &RaftService::StartElection); });
}

void RaftService::StopElectionTimer() {
  if (is_election_timer_start_) {
    LOG_TRACE << "StopElectionTimer, current term " << current_term_;
    system()->RemoveTimer(election_timer_h_);
    election_timer_h_.reset();
    is_election_timer_start_ = false;
  }
}

void RaftService::StartElection() {
  if (IsLeader())
    return;

  ++current_term_;
  StepDown(current_term_);

  LOG_INFO << "StartElection, current term " << current_term_;

  CHECK_EQ(state_, kState_Follower);

  voted_for_ = server_id_;
  state_ = kState_Candidate;
  leader_id_.clear();
  configuration_.GrantedVote(server_id_);
  CHECK(PersistMetaData());

  for (auto& h : peer_srv_handles_) {
    system()->Signal(h);
  }

  const bool granted_quorum_vote = configuration_.QuorumAll(
      [](const ServerInfo& sinfo) -> bool { return sinfo.granted_vote; });

  if (granted_quorum_vote) {
    BecomeLeader();
  } else {
    RestartElectionTimer();
  }
}

void RaftService::HandleRequestVote(const ::craft::RequestVoteRequest* request,
                                    ::craft::RequestVoteResponse* response) {
  LOG_INFO << "HandleRequestVote: current term " << current_term_ << ",server id "
           << request->server_id() << ",peer id " << request->peer_id() << ",term "
           << request->term() << ",last log term " << request->last_log_term()
           << ",last log index " << request->last_log_index() << ",voted for "
           << voted_for_;

  response->set_term(current_term_);
  response->set_granted(false);

  if (request->term() < current_term_) {
    LOG_INFO << "received a stale request,term " << request->term();
    return;
  }

  if (request->term() > current_term_) {
    StepDown(request->term());
  }

  assert(current_term_ == request->term());
  bool const is_log_ok = LastLogTerm() < request->last_log_term() ||
                         (LastLogTerm() == request->last_log_term() &&
                          LastLogIndex() <= request->last_log_index());

  if (is_log_ok && voted_for_.empty()) {
    RestartElectionTimer();
    voted_for_ = request->server_id();
    CHECK(PersistMetaData());
  }

  response->set_term(current_term_);
  response->set_granted(current_term_ == request->term() &&
                        voted_for_ == request->server_id());

  LOG_INFO << "HandleRequestVote: "
           << "is_log_ok " << is_log_ok << ",granted " << response->granted();
}

void RaftService::OnRequestVoteResponse(const std::string& peer_id,
                                        const ::craft::RequestVoteRequest& request,
                                        const ::craft::RequestVoteResponse& response) {
  LOG_INFO << "OnRequestVoteResponse peer id " << peer_id << ", term " << response.term();
  CHECK_NE(server_id_, peer_id);

  if (request.term() != current_term_ || state_ != kState_Candidate)
    return;

  if (response.term() < current_term_) {
    LOG_INFO << "received a stale vote, term " << response.term();
    return;
  }

  if (response.term() > current_term_) {
    LOG_TRACE << "received a greater term " << response.term();
    StepDown(response.term());
    return;
  }

  if (response.granted()) {
    LOG_INFO << "vote granted by " << peer_id;

    configuration_.GrantedVote(peer_id);
    const bool granted_quorum_vote = configuration_.QuorumAll(
        [](const ServerInfo& sinfo) -> bool { return sinfo.granted_vote; });

    if (granted_quorum_vote) {
      BecomeLeader();
    } else {
      LOG_INFO << "vote rejected by " << peer_id;
    }
  }
}

void RaftService::BecomeLeader() {
  LOG_INFO << "######## " << server_id_
           << " BecomeLeader ########## \nleader current term " << current_term_
           << ",last_log_index " << LastLogIndex() << ",committed log index "
           << committed_index_ << ",applied log index " << applied_index_;

  CHECK_NE(state_, kState_Leader);
  CHECK_EQ(voted_for_, server_id_);
  CHECK(!server_id_.empty());

  StopElectionTimer();
  leader_id_ = server_id_;
  state_ = kState_Leader;

  // append a NOOP log to allow read-only operations
  LogEntry le;
  le.set_type(LogEntry::kNOOP);
  le.set_term(current_term_);
  AppendLog(le);  // will signal leader disk service
  RestartStepDownTimer();
  StartAppendEntries();
}

void RaftService::RestartStepDownTimer() {
  StopStepDownTimer();

  if (!IsLeader())
    return;

  is_stepdown_timer_start_ = true;
  auto sys = this->system();
  auto h = handle();
  ++current_epoch_;
  configuration_.UpdateEpoch(server_id_, current_epoch_);

  auto epoch = current_epoch_;
  stepdown_timer_h_ = system()->AddTimer(election_timeout_ms_, [sys, h, epoch]() {
    sys->AsyncCallMethod(h, &RaftService::CheckStepDown, epoch);
  });

  // StartAppendEntries();
}

void RaftService::StopStepDownTimer() {
  if (is_stepdown_timer_start_) {
    system()->RemoveTimer(stepdown_timer_h_);
    stepdown_timer_h_.reset();
    is_stepdown_timer_start_ = false;
  }
}

void RaftService::CheckStepDown(int64_t epoch) {
  if (!IsLeader())
    return;

  const int64_t match_epoch = configuration_.QuorumMin(
      [](const ServerInfo& sinfo) -> int64_t { return sinfo.epoch; });

  if (match_epoch < epoch) {
    LOG_INFO << "CheckStepDown: append heartbeat entry timeout, current term "
             << current_term_;
    StepDown(current_term_);
    RestartElectionTimer();
  } else {
    RestartStepDownTimer();
  }
}

void RaftService::RestartAppendEntriesTimer() {
  //  LOG_INFO << "RestartAppendEntriesTimer";
  StopAppendEntriesTimer();
  if (!IsLeader())
    return;

  auto sys = system();
  auto h = handle();
  is_append_entries_timer_start_ = true;
  append_entries_timer_h_ = system()->AddTimer(
      append_entries_heartbeat_period_ms_,
      [sys, h]() { sys->AsyncCallMethod(h, &RaftService::StartAppendEntries); });
}

void RaftService::StopAppendEntriesTimer() {
  if (is_append_entries_timer_start_) {
    // LOG_TRACE << "StopAppendEntriesTimer";
    system()->RemoveTimer(append_entries_timer_h_);
    is_append_entries_timer_start_ = false;
  }
}

void RaftService::StartAppendEntries() {
  if (!IsLeader())
    return;

  for (auto& h : peer_srv_handles_) {
    system()->Signal(h);
  }

  RestartAppendEntriesTimer();
}

void RaftService::HandleAppendEntries(const ::craft::AppendEntriesRequest* request,
                                      ::craft::AppendEntriesResponse* response) {
  LOG_TRACE << "HandleAppendEntries: current term " << current_term_ << ",server id "
            << request->server_id() << ",peer id " << request->peer_id() << ",term "
            << request->term() << ",prev log term " << request->prev_log_term()
            << ",prev log index " << request->prev_log_index() << ",committed index "
            << request->committed_index();

  CHECK_EQ(request->peer_id(), server_id_);
  response->set_term(current_term_);
  response->set_success(false);
  response->set_last_log_index(LastLogIndex());

  if (request->term() < current_term_)
    return;

  if (request->term() > current_term_) {
    StepDown(request->term());
    leader_id_ = request->server_id();
  } else {
    if (leader_id_.empty())
      leader_id_ = request->server_id();

    CHECK_EQ(leader_id_, request->server_id());
  }

  response->set_term(request->term());
  int64_t const request_prev_log_index = request->prev_log_index();
  if (request_prev_log_index > LastLogIndex()) {
    RestartElectionTimer();
    return;
  }

  if (request_prev_log_index > 0 &&
      GetLogEntry(request_prev_log_index)->term() != request->prev_log_term()) {
    RestartElectionTimer();
    return;
  }

  int i = 0;
  for (; i < request->entries_size(); ++i) {
    auto log_index = request_prev_log_index + i + 1;
    if (log_index <= LastLogIndex()) {
      if (GetLogEntry(log_index)->term() == request->entries(i).term())
        continue;
    }

    break;
  }

  int64_t const log_index = request_prev_log_index + i + 1;
  if (log_index <= LastLogIndex()) {
    for (auto j = log_index; j <= LastLogIndex(); ++j) {
      auto it = command_closures_.find(j);
      if (it != command_closures_.end()) {
        Result* result = it->second.first;
        result->type = Result::kNotLeader;
        result->response = leader_id_;

        auto& closure = it->second.second;
        closure();
        command_closures_.erase(it);
      }
    }

    size_t new_log_size =
        NumOfLogEntries() - static_cast<size_t>((LastLogIndex() - log_index + 1));
    log_.PopBackLogEntries(NumOfLogEntries() - new_log_size);
  } else {
    CHECK_EQ(log_index, LastLogIndex() + 1);
  }

  mcast::Timer timer;
  timer.Start();
  for (; i < request->entries_size(); ++i) {
    if (request->entries(i).type() == LogEntry::kLock) {
      LockRequest e;
      CHECK(e.ParseFromString(request->entries(i).log_data()));
      e.set_time_ms(e.time_ms() + mcast::NowMilliseconds());

      LOG_INFO << "HandleAppendEntries: append a lock entry " << e.key() << " "
               << e.owner() << " " << mcast::TimeMsToString(e.time_ms());

      LogEntry log_e;
      log_e.set_type(LogEntry::kLock);
      log_e.set_term(request->entries(i).term());
      CHECK(e.SerializeToString(log_e.mutable_log_data()));
      log_.AppendLogEntry(log_e);
    } else {
      log_.AppendLogEntry(request->entries(i));
    }
  }

  if (request->entries_size() > 0) {
    CHECK(log_.Sync());
    LOG_INFO << "HandleAppendEntries: log append & sync spent " << std::fixed
             << timer.Elapsed().ToSeconds() << " seconds";

    LOG_INFO << "append log: current term " << current_term_ << ",entries "
             << request->entries_size();
  }

  if (request->committed_index() > committed_index_ &&
      request->committed_index() <= LastLogIndex()) {
    LOG_INFO << "HandleAppendEntries: change committed log  from " << committed_index_
             << " to " << request->committed_index();
    committed_index_ = request->committed_index();

    if (state_machine_h_) {
      system()->AsyncCallMethod(state_machine_h_, &StateMachineService::OnLogCommitted,
                                current_term_, committed_index_);
    }
  }

  configuration_.UpdateMatchLog(server_id_, LastLogIndex());
  response->set_success(true);
  response->set_last_log_index(LastLogIndex());
  RestartElectionTimer();
}

void RaftService::OnAppendEntriesResponse(const std::string& peer_id, int64_t epoch,
                                          const ::craft::AppendEntriesRequest& request,
                                          const ::craft::AppendEntriesResponse& response,
                                          bool* avail) {
  LOG_TRACE << "OnAppendEntriesResponse peer id " << peer_id << ",term "
            << response.term();

  CHECK_NE(server_id_, peer_id);
  *avail = false;

  if (request.term() != current_term_ || state_ != kState_Leader)
    return;

  if (response.term() < current_term_) {
    LOG_INFO << "received a stale AppendEntries message, term " << response.term();
    return;
  }

  if (response.term() > current_term_) {
    LOG_INFO << "received a greater term AppendEntries message, term " << response.term();
    StepDown(response.term());
    return;
  }

  if (response.success() && committed_index_ < response.last_log_index()) {
    configuration_.UpdateMatchLog(peer_id, response.last_log_index());
    AdvanceCommittedIndex();
  }

  configuration_.UpdateEpoch(peer_id, epoch);
  AdvanceEpoch();
  *avail = true;
}

void RaftService::OnLogSync(int64_t last_log_index_sync) {
  if (!IsLeader())
    return;

  LOG_INFO << "OnLogSync: last_log_index_sync " << last_log_index_sync;
  if (last_log_index_sync == LastLogIndex())
    log_sync_ = false;

  UpdateMatchLogIndex(last_log_index_sync);
  AdvanceCommittedIndex();
}

void RaftService::AdvanceCommittedIndex() {
  const int64_t new_committed_index = configuration_.QuorumMin(
      [](const ServerInfo& sinfo) -> int64_t { return sinfo.match_log; });

  if (new_committed_index > committed_index_ && new_committed_index <= LastLogIndex()) {
    LOG_INFO << "AdvanceCommittedIndex: change committed index from " << committed_index_
             << " to " << new_committed_index;
    committed_index_ = new_committed_index;
    if (state_machine_h_) {
      system()->AsyncCallMethod(state_machine_h_, &StateMachineService::OnLogCommitted,
                                current_term_, committed_index_);
    }
  }
}

int64_t RaftService::AppendLog(const std::string& log_data) {
  LogEntry le;
  le.set_type(LogEntry::kData);
  le.set_term(current_term_);
  le.set_log_data(log_data);
  return AppendLog(le);
}

int64_t RaftService::AppendLog(const LogEntry& e) {
  CHECK(IsLeader());
  log_.AppendLogEntry(e);
  log_sync_ = true;
  system()->Signal(leader_disk_h_);
  return LastLogIndex();
}

bool RaftService::PersistMetaData() {
  LOG_TRACE << "PersistMetaData: current term " << current_term_ << ",voted for "
            << voted_for_;

  mcast::Timer timer;
  timer.Start();
  if (log_.StoreMetaData(current_term_, voted_for_) && log_.SyncMetaData()) {
    LOG_INFO << "PersistMetaData spent " << std::fixed << timer.Elapsed().ToSeconds()
             << " seconds";
    return true;
  }

  return false;
}

void RaftService::GetLogEntries(int64_t log_index_start, int max_num,
                                std::vector<LogEntryPtr>* entries,
                                int64_t* last_log_index) {
  CHECK(log_index_start > 0);
  for (int64_t i = log_index_start; max_num > 0 && i <= LastLogIndex(); ++i, --max_num) {
    entries->push_back(log_.GetLogEntry(i));
  }

  *last_log_index = LastLogIndex();
}

void RaftService::GetLogEntry(int64_t log_index, LogEntryPtr* log_entry) {
  CHECK_GT(log_index, 0);
  CHECK_LE(log_index, LastLogIndex());

  *log_entry = log_.GetLogEntry(log_index);
}

void RaftService::OnLogExecuted(LogEntry::LogEntryType log_type, int64_t log_index,
                                const std::string& response) {
  LOG_INFO << "OnLogExecuted: log_index " << log_index;

  CHECK_GT(log_index, 0);
  CHECK_LE(log_index, LastLogIndex());
  CHECK_LE(applied_index_, log_index);

  applied_index_ = log_index;
  if (log_type == LogEntry::kLock) {
    ::craft::LockRequest e;
    CHECK(e.ParseFromString(GetLogEntry(log_index)->log_data()));
    auto& p = locks_[e.key()];
    p.first = e.owner();
    p.second = e.time_ms();

    LockResponse lock_res;
    lock_res.set_key(e.key());
    lock_res.set_owner(e.owner());
    lock_res.set_acquired(true);
    OnCommandDone(applied_index_, &lock_res);

    LOG_INFO << "locked " << e.key() << " " << e.owner() << " until "
             << mcast::TimeMsToString(e.time_ms());

  } else if (log_type == LogEntry::kUnlock) {
    ::craft::UnlockRequest e;
    CHECK(e.ParseFromString(GetLogEntry(log_index)->log_data()));

    UnlockResponse unlock_res;
    unlock_res.set_key(e.key());
    unlock_res.set_owner(e.owner());
    unlock_res.set_released(false);

    auto it = locks_.find(e.key());
    if (it != locks_.end() && it->second.first == e.owner()) {
      locks_.erase(e.key());
      unlock_res.set_released(true);
      LOG_INFO << "unlock " << e.key() << " " << e.owner();
    }

    OnCommandDone(applied_index_, &unlock_res);
  } else {
    Result result;
    result.type = Result::kResponse;
    OnCommandDone(applied_index_, Result::kResponse, response);
  }

  auto const rit = readonly_command_closures_.upper_bound(applied_index_);
  for (auto i = readonly_command_closures_.begin(); i != rit; ++i) {
    LOG_INFO << "Readonly command ok, log committed index " << i->first;
    rit->second.first->type = Result::kResponse;
    i->second.second();
  }

  readonly_command_closures_.erase(readonly_command_closures_.begin(), rit);
}

void RaftService::OnCommandDone(int64_t log_index, Result::Type type,
                                const std::string& response) {
  auto it = command_closures_.find(log_index);
  if (it != command_closures_.end()) {
    Result* result = it->second.first;
    result->type = type;
    result->response = response;
    auto& closure = it->second.second;
    closure();

    command_closures_.erase(it);
  }
}

void RaftService::OnCommandDone(int64_t log_index,
                                ::google::protobuf::Message* response) {
  auto it = command_closures_.find(log_index);
  if (it != command_closures_.end()) {
    Result* result = it->second.first;
    result->type = Result::kFailed;

    if (response->SerializeToString(&result->response)) {
      result->type = Result::kResponse;
    }
    auto& closure = it->second.second;
    closure();

    command_closures_.erase(it);
  }
}

bool RaftService::CheckIsLeader(Result* result, const Closure& closure) {
  if (!IsLeader()) {
    LOG_WARN << "CheckIsLeader: This server is not ther leader server, leader is "
             << leader_id_;

    result->type = Result::kFailed;
    if (!leader_id_.empty()) {
      result->type = Result::kNotLeader;
      result->response = leader_id_;
    }
    closure();
    return false;
  }

  return true;
}

void RaftService::ExecuteCommand(const std::string& request, Result* result,
                                 const Closure& closure) {
  LOG_INFO << "ExecuteCommand request size " << request.size();
  if (!CheckIsLeader(result, closure))
    return;

  CHECK(server_id_ == leader_id_ && current_term_ > 0);
  auto log_index = AppendLog(request);
  auto res = command_closures_.emplace(log_index, std::make_pair(result, closure));
  CHECK(res.second);
  StartAppendEntries();
}

void RaftService::AdvanceEpoch() {
  const int64_t match_epoch = configuration_.QuorumMin(
      [](const ServerInfo& sinfo) -> int64_t { return sinfo.epoch; });

  auto end = leader_uptodate_callback_.upper_bound(match_epoch);
  for (auto i = leader_uptodate_callback_.begin(); i != end; ++i) {
    i->second(true);
  }

  leader_uptodate_callback_.erase(leader_uptodate_callback_.begin(), end);
}

void RaftService::WaitRead(Result* result, const Closure& closure) {
  LOG_INFO << "WaitRead: current term " << current_term_ << ",committed index "
           << committed_index_ << ",applied index " << applied_index_ << ",current_epoch "
           << current_epoch_;

  if (!CheckIsLeader(result, closure))
    return;

  CHECK(server_id_ == leader_id_ && current_term_ > 0);
  ++current_epoch_;
  configuration_.UpdateEpoch(server_id_, current_epoch_);

  int64_t const loc_committed_index = committed_index_;
  auto res = leader_uptodate_callback_.emplace(
      current_epoch_, [this, result, closure, loc_committed_index](bool leader_uptodate) {
        if (leader_uptodate) {
          if (loc_committed_index <= applied_index_) {
            result->type = Result::kResponse;
            closure();

            LOG_INFO << "WaitRead ok, log committed index " << loc_committed_index;
          } else {
            readonly_command_closures_.emplace(loc_committed_index,
                                               std::make_pair(result, closure));
          }
        } else {
          result->type = Result::kFailed;
          closure();
        }
      });

  CHECK(res.second);
  AdvanceEpoch();
  StartAppendEntries();
}

void RaftService::Lock(const LockRequest& lock_req_org, Result* lock_result,
                       const Closure& closure) {
  LOG_INFO << "start to lock " << lock_req_org.key() << " " << lock_req_org.owner() << " "
           << lock_req_org.time_ms();
  if (!CheckIsLeader(lock_result, closure))
    return;

  lock_result->type = Result::kFailed;
  auto it = locks_.find(lock_req_org.key());
  if (it != locks_.end()) {
    LOG_INFO << it->first << " already locked by " << it->second.first << " until "
             << mcast::TimeMsToString(it->second.second);

    if (it->second.first != lock_req_org.owner() &&
        mcast::NowMilliseconds() < it->second.second) {  // already locked by other

      LOG_INFO << "failed to lock " << lock_req_org.key() << " " << lock_req_org.owner()
               << " " << lock_req_org.time_ms();

      std::string buf;
      LockResponse lock_res;
      lock_res.set_key(lock_req_org.key());
      lock_res.set_owner(it->second.first);
      lock_res.set_acquired(false);
      if (lock_res.SerializeToString(&lock_result->response)) {
        lock_result->type = Result::kResponse;
      }
      closure();
      return;
    }
  }

  LogEntry le;
  le.set_type(LogEntry::kLock);
  le.set_term(current_term_);

  LockRequest lock_req = lock_req_org;
  lock_req.set_time_ms(mcast::NowMilliseconds() + lock_req_org.time_ms());
  if (lock_req.SerializeToString(le.mutable_log_data())) {
    locks_[lock_req.key()] = {lock_req.owner(),
                              lock_req.time_ms()};  // locked by this server

    const int64_t log_index = AppendLog(le);  // will signal leader disk service

    auto lock_closure = [this, lock_req_org, lock_result, closure]() {
      if (lock_result->type != Result::kResponse) {
        LOG_INFO << "failed to lock " << lock_req_org.key() << " " << lock_req_org.owner()
                 << " " << lock_req_org.time_ms();
        this->locks_.erase(lock_req_org.key());
      }
      closure();
    };

    auto res =
        command_closures_.emplace(log_index, std::make_pair(lock_result, lock_closure));
    CHECK(res.second);
    StartAppendEntries();
  } else {
    closure();
  }
}

void RaftService::Unlock(const UnlockRequest& unlock_req, Result* result,
                         const Closure& closure) {
  LOG_INFO << "start to unlock " << unlock_req.key() << " " << unlock_req.owner();
  if (!CheckIsLeader(result, closure))
    return;

  result->type = Result::kFailed;
  LogEntry le;
  le.set_type(LogEntry::kUnlock);
  le.set_term(current_term_);

  if (unlock_req.SerializeToString(le.mutable_log_data())) {
    int64_t log_index = AppendLog(le);  // will signal leader disk service
    auto res = command_closures_.emplace(log_index, std::make_pair(result, closure));
    CHECK(res.second);
    StartAppendEntries();
  } else {
    LOG_INFO << "failed to unlock " << unlock_req.key() << " " << unlock_req.owner();
    closure();
  }
}

}  // namespace craft
