#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "service.pb.h"

namespace craft {
class RaftServer;
}

class KVDBService : public kvdb::ProtoKVDBService {
 public:
  explicit KVDBService(craft::RaftServer *srv) : raft_srv_(srv) {}
  ~KVDBService() override {}

  void Get(::google::protobuf::RpcController *controller,
           const ::kvdb::GetRequest *request, ::kvdb::GetResponse *response,
           ::google::protobuf::Closure *done) override;

  void Put(::google::protobuf::RpcController *controller,
           const ::kvdb::PutRequest *request, ::kvdb::PutResponse *response,
           ::google::protobuf::Closure *done) override;

  void Lock(::google::protobuf::RpcController *controller,
            const ::kvdb::LockRequest *request, ::kvdb::Response *response,
            ::google::protobuf::Closure *done) override;

  void Unlock(::google::protobuf::RpcController *controller,
              const ::kvdb::UnlockRequest *request, ::kvdb::Response *response,
              ::google::protobuf::Closure *done) override;

 private:
  craft::RaftServer *raft_srv_ = nullptr;
};
