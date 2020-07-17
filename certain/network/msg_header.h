#pragma once

#include "utils/header.h"

namespace certain {

const uint16_t kMagicNum = 0xac81;
const int kMsgHeaderSize = 14;

struct MsgHeader {
  // magic_num equals to kMagicNum
  uint16_t magic_num;

  uint8_t msg_id;

  // The reserved bits are in this field also.
  // (TODO)rock: Add use_checksum bit.
  uint8_t version;

  uint32_t len;
  uint32_t checksum;
  uint16_t result;

  MsgHeader() = delete;
  MsgHeader(uint8_t _msg_id);

  // The caller guarantee that the buffer has enough room.
  void SerializeTo(char* buffer);

  // The caller guarantee that the buffer's length not smaller than
  // kMsgHeaderSize.
  void ParseFrom(const char* buffer);
};

}  // namespace certain
