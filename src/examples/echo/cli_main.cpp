#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <set>
#include <string>
#include <thread>

#include "service.pb.h"

#include "mcast/RpcChannel.h"
#include "mcast/RpcServer.h"
#include "mcast/System.h"

#include "Configuration.h"
#include "RaftServer.h"

mcast::System sys;
mcast::RpcServer rpc_server(&sys);

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "usage: server_id ..." << std::endl;
    return -1;
  }

  std::set<std::string> server_ids;
  for (int i = 1; i < argc; ++i) {
    server_ids.insert(argv[i]);
  }

  std::cout << "echo client start " << *server_ids.begin() << std::endl;

  mcast::RpcChannel rcp_channel;
  rcp_channel.SetConnectParameter(*server_ids.begin());
  echo::ProtoEchoService::Stub stub(&rcp_channel);

  std::string echo_txt;
  int64_t sid = 0;
  ptrdiff_t server_index = 0;

  while (std::cin >> echo_txt) {
    if (echo_txt == "q") {
      break;
    }
    ::echo::Request request;
    ::echo::Response response;
    request.set_sequence_num(++sid);
    request.set_request(echo_txt);

    while (true) {
      mcast::RpcController controller;
      mcast::NullClosure done;
      stub.Execute(&controller, &request, &response, &done);

      if (!controller.Failed()) {
        if (response.success()) {
          if (response.sequence_num() == sid) {
            std::cout << response.response() << " OK!" << std::endl;
            break;
          } else {
            std::cout << "received a stale cmd, sid" << response.sequence_num()
                      << ",request " << echo_txt << std::endl;
          }
        } else {
          if (response.has_server_id()) {
            server_ids.insert(response.server_id());
            rcp_channel.SetConnectParameter(response.server_id());
            std::cout << "change server to " << response.server_id() << std::endl;
          }
        }
      } else {
        ++server_index;
        server_index %= server_ids.size();
        auto srvid = *std::next(server_ids.begin(), server_index);
        std::cout << "try connect server " << srvid << std::endl;
        rcp_channel.SetConnectParameter(srvid);
      }

      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
  }

  rpc_server.Stop();
  sys.Stop();
  return 0;
}
