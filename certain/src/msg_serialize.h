#pragma once

#include <string>

#include "network/msg_channel.h"
#include "src/command.h"

namespace certain {

class MsgSerialize : public SerializeCallBackBase {
 private:
  MsgBase* cmd_ = nullptr;

 public:
  MsgSerialize(MsgBase* cmd) : cmd_(cmd) {}

  virtual uint8_t GetMsgId() override { return cmd_->cmd_id(); }

  virtual int32_t ByteSize() override { return cmd_->SerializedByteSize(); }

  virtual bool SerializeTo(char* buffer, uint32_t len);
};

}  // namespace certain
