#include "msg_serialize.h"

namespace certain {

bool MsgSerialize::SerializeTo(char* buffer, uint32_t len) {
  uint32_t real_len = 0;
  bool ok = cmd_->SerializeToBuffer(buffer, len, real_len);
  if (!ok) {
    CERTAIN_LOG_FATAL("serialize to buffer failed");
  }
  assert(real_len == len);
  return true;
}

}  // namespace certain
