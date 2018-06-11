
include_directories(.)

set(SOURCE 
  PeerService.cpp 
  RaftService.cpp 
  RaftServer.cpp 
  Log.cpp 
  StateMachineService.cpp 
  ProxyService.cpp 
  raft.pb.cc
)

add_library(craft STATIC ${SOURCE})

add_executable(echo_srv 
  examples/echo/EchoService.cpp 
  examples/echo/service.pb.cc 
  examples/echo/srv_main.cpp 
)
target_link_libraries (echo_srv craft libmcast.a protobuf)

add_executable(echo_cli 
  examples/echo/service.pb.cc 
  examples/echo/cli_main.cpp
)
target_link_libraries (echo_cli craft libmcast.a protobuf )

add_executable(kvdb_srv 
  examples/kvdb/KVDBService.cpp 
  examples/kvdb/service.pb.cc
  examples/kvdb/srv_main.cpp)
target_link_libraries (kvdb_srv craft libmcast.a protobuf)

add_executable(kvdb_cli 
  examples/kvdb/service.pb.cc
  examples/kvdb/cli_main.cpp
)
target_link_libraries (kvdb_cli craft libmcast.a protobuf)

file(GLOB HEADERS "*.h")
install(FILES ${HEADERS} DESTINATION include/craft)

install(TARGETS craft DESTINATION lib)











