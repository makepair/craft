#include "PeerService.h"

#include "raft.pb.h"

#include "mcast/RpcClosure.h"
#include "mcast/RpcServer.h"
#include "mcast/util/DateTime.h"
#include "mcast/util/Logging.h"

#include "RaftService.h"
#include "StateMachineService.h"

namespace craft {

PeerService::PeerService(mcast::System *sys, const std::string &name,
                         const mcast::ServiceHandle &raft_service_h,
                         const std::string &server_id, const std::string &peer_id)
    : mcast::UserThreadService(sys, name + ':' + peer_id),
      raft_service_h_(raft_service_h),
      server_id_(server_id),
      peer_id_(peer_id),
      rcp_channel_(this) {
  rcp_channel_.SetConnectParameter(peer_id);
}

void PeerService::OnServiceStart() {
  LOG_INFO << "peer service " << peer_id_ << " start";
}

void PeerService::OnServiceStop() {
  LOG_INFO << "peer service " << peer_id_ << "stop";
}

void PeerService::Main() {
  int64_t current_term = 0;
  int64_t current_epoch = 0;
  int64_t committed_index = 0;
  int64_t last_log_term = 0;
  int64_t last_log_index = 0;
  RaftService::State state = RaftService::kState_Invail;

  while (!this->IsStopping()) {
    if (!system()->CallMethod(raft_service_h_, &RaftService::GetState, &state,
                              &current_term, &current_epoch, &committed_index,
                              &last_log_index, &last_log_term)) {
      break;
    }

    switch (state) {
      case RaftService::kState_Follower:
        next_log_ = last_log_index + 1;
        prev_log_term_ = last_log_term;
        break;

      case RaftService::kState_Candidate:
        next_log_ = last_log_index + 1;
        prev_log_term_ = last_log_term;
        RequestVote(current_term, last_log_index, last_log_term);
        break;

      case RaftService::kState_Leader:
        if (next_log_ == 0) {
          next_log_ = last_log_index + 1;
          prev_log_term_ = last_log_term;
        }

        AppendEntries(current_term, current_epoch, committed_index);
        if (last_log_index > 1 && next_log_ <= last_log_index) {
          continue;
        }
        break;
      default:
        assert(false);
    }

    system()->WaitSignal();
  }
}

void PeerService::RequestVote(int64_t term, int64_t last_log_index,
                              int64_t last_log_term) {
  LOG_INFO << "RequestVote: term " << term << ",server id " << server_id_ << ",peer id "
           << peer_id_ << ",last log term " << last_log_term << ",last log index "
           << last_log_index;

  ::craft::RpcRaftService::Stub stub(&rcp_channel_);
  ::craft::RequestVoteRequest request;
  ::craft::RequestVoteResponse response;

  request.set_server_id(server_id_);
  request.set_peer_id(peer_id_);
  request.set_term(term);
  request.set_last_log_term(last_log_term);
  request.set_last_log_index(last_log_index);

  mcast::RpcController controller;
  mcast::NullClosure done;
  stub.RequestVote(&controller, &request, &response, &done);

  CHECK(request.term() == term);

  if (!controller.Failed()) {
    system()->CallMethod(raft_service_h_, &RaftService::OnRequestVoteResponse, peer_id_,
                         request, response);
  } else {
    LOG_TRACE << controller.ErrorText();
  }
}

void PeerService::AppendEntries(int64_t current_term, int64_t epoch,
                                int64_t committed_index) {
  std::vector<LogEntryPtr> entries;
  int const max_num_per_loop = 32;
  CHECK(raft_service_h_);
  int64_t last_log_index = 0;
  auto r = system()->CallMethod(raft_service_h_, &RaftService::GetLogEntries, next_log_,
                                max_num_per_loop, &entries, &last_log_index);
  if (!r) {
    LOG_INFO << "CallMethod failed: " << r.ErrorText();
    return;
  }
  ::craft::AppendEntriesRequest request;
  ::craft::AppendEntriesResponse response;
  if (!RPCAppendEntriesToPeer(current_term, committed_index, entries, &request,
                              &response)) {
    return;
  }

  bool avail = false;
  if (!system()->CallMethod(raft_service_h_, &RaftService::OnAppendEntriesResponse,
                            peer_id_, epoch, request, response, &avail)) {
    LOG_INFO << "CallMethod failed: " << r.ErrorText();
    return;
  }
  if (avail) {
    if (response.success()) {
      next_log_ = response.last_log_index() + 1;
      if (!entries.empty()) {
        prev_log_term_ = entries.back()->term();
        LOG_INFO << "append log to " << peer_id_ << " ok, current term " << current_term
                 << ",match log " << next_log_ - 1 << ",next log " << next_log_
                 << ",num of entries " << entries.size();
      }
    } else {
      if (next_log_ > 1) {
        --next_log_;
        LOG_INFO << "decrease next log to " << next_log_;
      }

      int64_t const prev_log_index = next_log_ - 1;
      LogEntryPtr prev_log_entry;
      if (prev_log_index > 0) {
        if (system()->CallMethod(raft_service_h_, &RaftService::GetLogEntry,
                                 prev_log_index, &prev_log_entry)) {
          prev_log_term_ = prev_log_entry->term();
        } else {
          next_log_ = 0;
          prev_log_term_ = 0;
        }
      } else {
        prev_log_term_ = 0;
      }
    }
  }
}

bool PeerService::RPCAppendEntriesToPeer(int64_t term, int64_t committed_index,
                                         const std::vector<LogEntryPtr> &entries,
                                         ::craft::AppendEntriesRequest *request,
                                         ::craft::AppendEntriesResponse *response) {
  //  LOG_TRACE << "RPCAppendEntriesToPeer num entries " << entries.size();
  ::craft::RpcRaftService::Stub stub(&rcp_channel_);
  request->set_server_id(server_id_);
  request->set_peer_id(peer_id_);
  request->set_term(term);
  request->set_prev_log_term(PrevLogTerm());
  request->set_prev_log_index(PrevLogIndex());
  request->set_committed_index(committed_index);
  for (auto &e : entries) {
    auto *pe = request->add_entries();
    if (e->type() == LogEntry::kLock) {
      pe->set_type(LogEntry::kLock);
      pe->set_term(e->term());

      LockRequest lock_e;
      CHECK(lock_e.ParseFromString(e->log_data()));
      lock_e.set_time_ms(lock_e.time_ms() - mcast::NowMilliseconds());
      CHECK(lock_e.SerializeToString(pe->mutable_log_data()));
    } else {
      *pe = *e;
    }
  }

  mcast::RpcController controller;
  mcast::NullClosure done;
  stub.AppendEntries(&controller, request, response, &done);

  if (controller.Failed()) {
    LOG_TRACE << controller.ErrorText();
  } else {
    static int count = 0;

    if (!entries.empty()) {
      LOG_INFO << "append entries to peer " << peer_id_ << ",next log index " << next_log_
               << ",prev log term " << prev_log_term_;
    } else if (++count > 50) {
      LOG_INFO << "append heartbeat entry to peer " << peer_id_ << ",next log index "
               << next_log_ << ",prev log term " << prev_log_term_;

      count = 0;
    }
  }

  return !controller.Failed();
}

}  // namespace craft
