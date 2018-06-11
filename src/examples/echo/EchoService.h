#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "service.pb.h"

namespace craft {
class RaftServer;
}

class EchoService : public echo::ProtoEchoService {
 public:
  explicit EchoService(craft::RaftServer *srv) : raft_srv_(srv) {}
  ~EchoService() override {}

  void Execute(::google::protobuf::RpcController *controller,
               const ::echo::Request *request,
               ::echo::Response *response,
               ::google::protobuf::Closure *done) override;

 private:
  craft::RaftServer *raft_srv_ = nullptr;
};
