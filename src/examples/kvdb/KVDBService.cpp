#include "KVDBService.h"

#include "RaftServer.h"

#include "KVDB.h"
#include "Result.h"

using namespace craft;

void KVDBService::Get(::google::protobuf::RpcController *controller,
                      const ::kvdb::GetRequest *request, ::kvdb::GetResponse *response,
                      ::google::protobuf::Closure *done) {
  mcast::Status status;
  craft::Result *result = new craft::Result();
  status = raft_srv_->AsyncWaitRead(result, [request, response, result, done]() {
    response->set_success(0);
    response->set_sequence_num(request->sequence_num());
    response->set_result(0);
    if (result->type == Result::kNotLeader) {
      response->set_server_id(result->response);
    } else if (result->type == Result::kResponse) {
      response->set_success(true);
      std::string value;
      if (KVDB::Instance().Get(request->key(), &value)) {
        response->set_result(1);
        response->set_value(value);
      }
    }

    delete result;
    done->Run();
  });

  if (!status) {
    delete result;
    controller->SetFailed(status.ErrorText());
    done->Run();
  }
}

void KVDBService::Put(::google::protobuf::RpcController *controller,
                      const ::kvdb::PutRequest *request, ::kvdb::PutResponse *response,
                      ::google::protobuf::Closure *done) {
  mcast::Status status;
  craft::Result *result = new craft::Result();
  status = raft_srv_->AsyncExecuteCommand(
      request, result, [request, response, result, done]() {
        response->set_success(false);
        response->set_sequence_num(request->sequence_num());
        response->set_result(0);

        if (result->type == Result::kNotLeader) {
          response->set_server_id(result->response);
        } else if (result->type == Result::kResponse) {
          response->set_success(true);
          if (response->ParseFromString(result->response)) {
            response->set_result(1);
            response->set_sequence_num(request->sequence_num());
          }
        }

        delete result;
        done->Run();
      });

  if (!status) {
    delete result;
    controller->SetFailed(status.ErrorText());
    done->Run();
  }
}

void KVDBService::Lock(::google::protobuf::RpcController *controller,
                       const ::kvdb::LockRequest *request, ::kvdb::Response *response,
                       ::google::protobuf::Closure *done) {
  craft::Result *result = new craft::Result();
  response->set_success(false);
  response->set_sequence_num(request->sequence_num());
  result->type = Result::kFailed;
  craft::LockRequest lock_req;
  lock_req.set_key(request->key());
  lock_req.set_owner(request->owner());
  lock_req.set_time_ms(request->timeout_ms());
  auto status = raft_srv_->Lock(lock_req, result);
  if (status) {
    if (result->type == Result::kNotLeader) {
      response->set_server_id(result->response);
    } else if (result->type == Result::kResponse) {
      craft::LockResponse res;
      if (res.ParseFromString(result->response)) {
        response->set_success(true);
        response->set_result(res.acquired());
      }
    }
  }

  if (!status) {
    controller->SetFailed(status.ErrorText());
  }

  delete result;
  done->Run();
}

void KVDBService::Unlock(::google::protobuf::RpcController *controller,
                         const ::kvdb::UnlockRequest *request, ::kvdb::Response *response,
                         ::google::protobuf::Closure *done) {
  craft::Result *result = new craft::Result();
  response->set_success(false);
  response->set_sequence_num(request->sequence_num());
  result->type = Result::kFailed;

  craft::UnlockRequest unlock_req;
  unlock_req.set_key(request->key());
  unlock_req.set_owner(request->owner());
  auto status = raft_srv_->Unlock(unlock_req, result);
  if (status) {
    if (result->type == Result::kNotLeader) {
      response->set_server_id(result->response);
    } else if (result->type == Result::kResponse) {
      craft::UnlockResponse res;
      if (res.ParseFromString(result->response)) {
        response->set_success(true);
        response->set_result(res.released());
      }
    }
  }

  if (!status) {
    controller->SetFailed(status.ErrorText());
  }

  delete result;
  done->Run();
}
