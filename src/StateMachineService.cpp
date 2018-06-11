#include "StateMachineService.h"

#include "mcast/util/Logging.h"

#include "RaftService.h"

namespace craft {

void StateMachineService::OnServiceStop() {
  StopStateMachine();
}

void StateMachineService::StartStateMachine(int64_t term) {
  LOG_INFO << "StartStateMachine crurent term " << term;
  current_term_ = term;
}

void StateMachineService::StopStateMachine() {
}

void StateMachineService::OnLogCommitted(int64_t term, int64_t committed_index) {
  LOG_INFO << "OnLogCommitted: change committed index from " << committed_index_ << " to "
           << committed_index << " in term " << term;

  current_term_ = term;

  CHECK_GT(committed_index, 0);
  CHECK_LE(committed_index_, committed_index);

  committed_index_ = committed_index;
  Apply();
}

void StateMachineService::Apply() {
  CHECK(state_machine_);
  int max_num_per_loop = 32;
  std::vector<LogEntryPtr> entries;
  int64_t last_log_index = 0;

  while (applied_index_ < committed_index_) {
    if (system()->CallMethod(raft_service_h_, &RaftService::GetLogEntries,
                             applied_index_ + 1, max_num_per_loop, &entries,
                             &last_log_index)) {
      LOG_INFO << "GetLog num " << entries.size();
      CHECK(!entries.empty());
      CHECK_LE(applied_index_, last_log_index);

      if (!Apply(entries)) {
        return;
      }
      entries.clear();
    } else {
      return;
    }
  }
}

bool StateMachineService::Apply(const std::vector<LogEntryPtr> &entries) {
  std::string response;
  for (size_t i = 0; i < entries.size(); ++i) {
    LOG_INFO << "apply term " << entries[i]->term() << ",applied index "
             << applied_index_ + 1;

    const LogEntryPtr &le = entries[i];
    if (le->type() == LogEntry::kData) {
      state_machine_->Execute(le->term(), applied_index_ + 1, le->log_data(), &response);
    }

    if (!system()->CallMethod(raft_service_h_, &RaftService::OnLogExecuted, le->type(),
                              applied_index_ + 1, response)) {
      LOG_WARN << "CallMethod OnLogExecuted failed";
      return false;
    }

    ++applied_index_;
    response.clear();
  }

  LOG_INFO << "Apply ok: committed index " << committed_index_ << ",applied index "
           << applied_index_;
  return true;
}

}  // namespace craft
