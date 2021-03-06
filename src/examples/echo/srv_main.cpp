#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "service.pb.h"

#include "mcast/RpcChannel.h"
#include "mcast/RpcServer.h"
#include "mcast/System.h"
#include "mcast/util/Logging.h"

#include "Configuration.h"
#include "EchoService.h"
#include "RaftServer.h"
#include "StateMachine.h"

craft::RaftServer* raft_srv_ptr = nullptr;

static void QuitHandler(int signo) {
  if (raft_srv_ptr)
    raft_srv_ptr->Stop();
}

struct EchoStateMachine : public craft::StateMachine {
  void Execute(int64_t term, int64_t log_index, const std::string& request,
               std::string* response) override {
    echo::Request rpc_request;
    echo::Response rpc_response;
    rpc_response.set_success(false);
    rpc_response.set_sequence_num(0);

    if (rpc_request.ParseFromString(request)) {
      rpc_response.set_success(true);
      rpc_response.set_sequence_num(rpc_request.sequence_num());
      rpc_response.set_response(rpc_request.request());

      LOG_INFO << "EchoStateMachine::Execute term " << term << ",log_index " << log_index
               << ",request " << rpc_request.request();
    } else {
      LOG_WARN << "ParseFromString failed";
    }

    if (!rpc_response.SerializeToString(response)) {
      LOG_WARN << "SerializeToString error";
      response->clear();
    }
  }
};

int main(int argc, char* argv[]) {
  mcast::SetLogLevel(mcast::LogLevel::kInfo);
  if (argc < 3) {
    LOG_INFO << "usage: log_folder this_server_id [peer_id]...\nexample: "
                "./echo_srv . 127.0.0.1:11111 127.0.0.1:11112";
    return -1;
  }

  const char* log_folder = argv[1];
  LOG_INFO << "log folder: " << log_folder;

  std::vector<std::string> server_ids;
  for (int i = 2; i < argc; ++i) {
    server_ids.push_back(argv[i]);
  }

  LOG_INFO << "echo server start " << server_ids[0];

  craft::RaftServer raft_srv;
  raft_srv_ptr = &raft_srv;

  craft::Configuration configuration;
  configuration.SetThisServerId(server_ids[0]);

  for (size_t i = 0; i < server_ids.size(); ++i) {
    LOG_INFO << "AddServerId " << server_ids[i];
    configuration.AddServerId(server_ids[i]);
  }

  signal(SIGINT, QuitHandler);
  signal(SIGTERM, QuitHandler);

  mcast::RpcServicePtr client_sevice_ptr(new EchoService(&raft_srv));
  if (raft_srv.Start(configuration, log_folder, std::make_shared<EchoStateMachine>(),
                     1000, 500)) {
    raft_srv.AddRpcService(client_sevice_ptr);

    raft_srv.Wait();
  } else {
    LOG_WARN << "Raft server start failed";
  }
  return 0;
}
