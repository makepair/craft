#pragma once

#include <stdint.h>
#include <vector>

#include "mcast/util/Noncopyable.h"

namespace craft {

struct StateMachine : public mcast::Noncopyable {
  StateMachine() = default;
  virtual ~StateMachine() {}

  virtual void Execute(int64_t term, int64_t log_index, const std::string &request,
                       std::string *response) = 0;
};

typedef std::shared_ptr<StateMachine> StateMachinePtr;

}  // namespace craft
