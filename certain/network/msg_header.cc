#include "network/msg_header.h"

namespace certain {

MsgHeader::MsgHeader(uint8_t _msg_id) {
  magic_num = kMagicNum;
  msg_id = _msg_id;
  version = 0;
  len = 0;
  checksum = 0;
  result = 0;
}

void MsgHeader::SerializeTo(char* buffer) {
  *(uint16_t*)buffer = htons(magic_num);
  *(uint8_t*)(buffer + 2) = msg_id;
  *(uint8_t*)(buffer + 3) = version;
  *(uint32_t*)(buffer + 4) = htonl(len);
  *(uint32_t*)(buffer + 8) = htonl(checksum);
  *(uint16_t*)(buffer + 12) = htons(result);
}

void MsgHeader::ParseFrom(const char* buffer) {
  magic_num = ntohs(*(const uint16_t*)buffer);
  msg_id = *(const uint8_t*)(buffer + 2);
  version = *(const uint8_t*)(buffer + 3);
  len = ntohl(*(const uint32_t*)(buffer + 4));
  checksum = ntohl(*(const uint32_t*)(buffer + 8));
  result = ntohs(*(const uint16_t*)(buffer + 12));
}

}  // namespace certain
