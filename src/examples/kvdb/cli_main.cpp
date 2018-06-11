#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

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

volatile bool quit = false;
int64_t sid = 0;

static bool DoGetRequest(mcast::RpcController* controller,
                         kvdb::ProtoKVDBService::Stub* stub, std::string key,
                         std::string* new_serverid) {
  mcast::NullClosure done;
  ::kvdb::GetRequest request;
  ::kvdb::GetResponse response;

  request.set_sequence_num(++sid);
  request.set_key(key);

  stub->Get(controller, &request, &response, &done);
  if (!controller->Failed()) {
    if (response.success()) {
      if (response.sequence_num() == sid) {
        if (response.result())
          std::cout << "get " << request.key() << " " << response.value() << " OK"
                    << std::endl;
        else
          std::cout << "get " << key << " failed" << std::endl;
        return true;
      } else {
        std::cout << "received a stale request, sid" << response.sequence_num()
                  << ",get key " << key << std::endl;
      }
    } else {
      if (response.has_server_id()) {
        *new_serverid = response.server_id();
      }
    }
  }

  return false;
}

static bool DoPutRequest(mcast::RpcController* controller,
                         kvdb::ProtoKVDBService::Stub* stub, std::string key,
                         std::string value, std::string* new_serverid) {
  mcast::NullClosure done;

  ::kvdb::PutRequest request;
  ::kvdb::PutResponse response;

  request.set_sequence_num(++sid);
  request.set_key(key);
  request.set_value(value);

  stub->Put(controller, &request, &response, &done);
  if (!controller->Failed()) {
    if (response.success()) {
      if (response.sequence_num() == sid) {
        if (response.result())
          std::cout << "put " << request.key() << " " << request.value() << " OK"
                    << std::endl;
        else
          std::cout << "put " << key << " " << value << ",failed" << std::endl;
        return true;
      } else {
        std::cout << "received a stale request, sid" << response.sequence_num()
                  << ",put key " << key << std::endl;
      }
    } else {
      if (response.has_server_id()) {
        *new_serverid = response.server_id();
      }
    }
  }

  return false;
}

static bool DoLockRequest(mcast::RpcController* controller,
                          kvdb::ProtoKVDBService::Stub* stub, std::string key,
                          std::string owner, int timeout_ms, std::string* new_serverid) {
  mcast::NullClosure done;
  ::kvdb::LockRequest request;
  ::kvdb::Response response;

  request.set_sequence_num(++sid);
  request.set_key(key);
  request.set_owner(owner);
  request.set_timeout_ms(timeout_ms);

  stub->Lock(controller, &request, &response, &done);
  if (!controller->Failed()) {
    if (response.success()) {
      if (response.sequence_num() == sid) {
        if (response.result())
          std::cout << "lock " << key << " " << owner << " OK" << std::endl;
        else
          std::cout << "acquired lock " << key << " " << owner << " failed" << std::endl;

        return true;
      } else {
        std::cout << "received a stale request, sid" << response.sequence_num()
                  << ",lock key " << key << std::endl;
      }
    } else {
      if (response.has_server_id()) {
        *new_serverid = response.server_id();
      }

      std::cout << "lock " << key << " " << owner << " failed" << std::endl;
    }
  }

  return false;
}

static bool DoUnlockRequest(mcast::RpcController* controller,
                            kvdb::ProtoKVDBService::Stub* stub, std::string key,
                            std::string owner, std::string* new_serverid) {
  mcast::NullClosure done;
  ::kvdb::UnlockRequest request;
  ::kvdb::Response response;

  request.set_sequence_num(++sid);
  request.set_key(key);
  request.set_owner(owner);

  stub->Unlock(controller, &request, &response, &done);
  if (!controller->Failed()) {
    if (response.success()) {
      if (response.sequence_num() == sid) {
        if (response.result())
          std::cout << "unlock " << key << " " << owner << " OK" << std::endl;
        else
          std::cout << "release lock " << key << " " << owner << " failed" << std::endl;
        return true;
      } else {
        std::cout << "received a stale request, sid" << response.sequence_num()
                  << ",unlock key " << key << std::endl;
      }
    } else {
      if (response.has_server_id()) {
        *new_serverid = response.server_id();
      }

      std::cout << "unlock " << key << " " << owner << " failed" << std::endl;
    }
  }

  return false;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "usage: server_id ..." << std::endl;
    return -1;
  }

  std::set<std::string> server_ids;
  for (int i = 1; i < argc; ++i) {
    server_ids.insert(argv[i]);
  }

  std::cout << "kvdb client start " << *server_ids.begin() << std::endl;

  mcast::RpcChannel rcp_channel;
  rcp_channel.SetConnectParameter(*server_ids.begin());
  kvdb::ProtoKVDBService_Stub stub(&rcp_channel);

  std::string new_serverid;
  std::string cmd;
  std::string key, value;
  std::string owner;
  int timeout_ms = 0;
  ptrdiff_t server_index = 0;

  while (std::cin >> cmd) {
    if (cmd == "q") {
      break;
    }

    mcast::RpcController controller;
    bool res = false;
    if (cmd == "get") {
      if (std::cin >> key)
        res = DoGetRequest(&controller, &stub, key, &new_serverid);
    } else if (cmd == "put") {
      if (std::cin >> key && std::cin >> value)
        res = DoPutRequest(&controller, &stub, key, value, &new_serverid);
    } else if (cmd == "lock") {
      if (std::cin >> key && std::cin >> owner && std::cin >> timeout_ms)
        res = DoLockRequest(&controller, &stub, key, owner, timeout_ms, &new_serverid);
    } else if (cmd == "unlock") {
      if (std::cin >> key && std::cin >> owner)
        res = DoUnlockRequest(&controller, &stub, key, owner, &new_serverid);
    } else {
      std::cout << "wrong command, usage: get key,put key value,lock key own ttl_ms"
                << std::endl;
      continue;
    }

    int wait_seconds = 1;
    while (!res) {
      controller.Reset();
      if (!new_serverid.empty()) {
        server_ids.insert(new_serverid);
        rcp_channel.SetConnectParameter(new_serverid);
        std::cout << "change server to " << new_serverid << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(wait_seconds));
      } else {
        ++server_index;
        server_index %= server_ids.size();
        auto srvid = *std::next(server_ids.begin(), server_index);
        std::cout << "try connect server , " << srvid << std::endl;
        rcp_channel.SetConnectParameter(srvid);
        std::this_thread::sleep_for(std::chrono::seconds(wait_seconds));
      }

      if (wait_seconds > 10)
        wait_seconds = 10;

      if (cmd == "get") {
        res = DoGetRequest(&controller, &stub, key, &new_serverid);
      } else if (cmd == "put") {
        res = DoPutRequest(&controller, &stub, key, value, &new_serverid);
      } else if (cmd == "lock") {
        res = DoLockRequest(&controller, &stub, key, owner, timeout_ms, &new_serverid);
      } else if (cmd == "unlock") {
        res = DoUnlockRequest(&controller, &stub, key, owner, &new_serverid);
      }
    }

    new_serverid.clear();
    cmd.clear();
    key.clear();
    value.clear();
  }

  rpc_server.Stop();
  sys.Stop();
  return 0;
}
