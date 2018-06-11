#pragma once

#include <string>

namespace craft {

struct Result {
  enum Type { kNone = 0, kResponse, kNotLeader, kFailed };
  Type type = kNone;
  std::string response;
};

}  // namespace craft
