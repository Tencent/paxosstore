#pragma once

#include "certain/errors.h"
#include "certain/monitor.h"
#include "certain/options.h"
#include "proto/certain.pb.h"

namespace certain {

const uint32_t kInvalidAcceptorId = UINT32_MAX;
const uint32_t kInvalidEntry = UINT32_MAX;

inline std::string EntryRecordToString(const EntryRecord& e) {
  std::string buffer(128, '\0');
  snprintf(&buffer[0], buffer.size(),
           "%u %u %u vid %lu u.sz %u v.sz %lu cho %u has %u", e.prepared_num(),
           e.promised_num(), e.accepted_num(), e.value_id(), e.uuids().size(),
           e.value().size(), e.chosen(), e.has_value_id_only());
  return buffer;
}

}  // namespace certain
