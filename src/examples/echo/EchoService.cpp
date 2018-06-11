#include "EchoService.h"

#include "RaftServer.h"
#include "Result.h"

void EchoService::Execute(::google::protobuf::RpcController *controller,
                          const ::echo::Request *request, ::echo::Response *response,
                          ::google::protobuf::Closure *closure) {
  craft::Result *result = new craft::Result();
  auto s = raft_srv_->AsyncExecuteCommand(
      request, result, [request, response, result, closure]() {

        response->set_success(false);
        response->set_sequence_num(request->sequence_num());
        response->set_success(result->type == craft::Result::kResponse);
        if (result->type == craft::Result::kResponse) {
          response->ParseFromString(result->response);
        } else if (result->type == craft::Result::kNotLeader) {
          response->set_server_id(result->response);
        }

        delete result;
        closure->Run();
      });

  if (!s) {
    delete result;
    controller->SetFailed(s.ErrorText());
    closure->Run();
  }
}
