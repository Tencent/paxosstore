#pragma once

#include "certain/monitor.h"
#include "certain/options.h"
#include "proto/certain.pb.h"
#include "src/common.h"
#include "utils/header.h"
#include "utils/log.h"
#include "utils/time.h"

namespace certain {

enum CmdIds {
  kCmdPaxos = 1,
  kCmdRangeCatchup = 2,
  kCmdReplay = 11,
  kCmdRead = 12,
  kCmdWrite = 13,
  kCmdRecover = 14,
  kCmdCatchup = 15,
  kCmdLoad = 16,
  kCmdDumpEntry = 21,
};

class CmdBase {
 protected:
  uint8_t cmd_id_;
  uint64_t uuid_ = 0;
  int result_ = 0;

  uint64_t entity_id_ = 0;
  uint64_t entry_ = 0;

  uint64_t timestamp_usec_ = GetTimeByUsec();

  void SetFromHeader(const CmdHeader* header) {
    uuid_ = header->uuid();
    entity_id_ = header->entity_id();
    entry_ = header->entry();
    result_ = header->result();
  }

  void SetToHeader(CmdHeader* header) {
    header->set_uuid(uuid_);
    header->set_entity_id(entity_id_);
    header->set_entry(entry_);
    header->set_result(result_);
  }

 public:
  CmdBase(uint8_t cmd_id) : cmd_id_(cmd_id) {}
  virtual ~CmdBase() {}

  CERTAIN_GET_SET(uint8_t, cmd_id);
  CERTAIN_GET_SET(uint64_t, uuid);
  CERTAIN_GET_SET(int32_t, result);

  CERTAIN_GET_SET(uint64_t, entity_id);
  CERTAIN_GET_SET(uint64_t, entry);

  CERTAIN_GET_SET(uint64_t, timestamp_usec);

  virtual std::string ToString() = 0;
};

class MsgBase : public CmdBase {
 public:
  MsgBase(uint8_t cmd_id) : CmdBase(cmd_id) {}
  virtual ~MsgBase() {}

  virtual bool ParseFromBuffer(const char* buffer, uint32_t len) = 0;
  virtual bool SerializeToBuffer(char* buffer, uint32_t len,
                                 uint32_t& real_len) = 0;
  virtual uint32_t SerializedByteSize() = 0;

  CERTAIN_GET_SET(uint32_t, local_acceptor_id);
  CERTAIN_GET_SET(uint32_t, peer_acceptor_id);

 protected:
  uint32_t local_acceptor_id_ = 0;
  uint32_t peer_acceptor_id_ = 0;
};

class PaxosCmd : public MsgBase {
 private:
  // For plog worker.
  bool plog_load_ = false;
  bool plog_range_load_ = false;
  bool plog_get_record_ = false;
  bool plog_set_record_ = false;
  bool plog_return_msg_ = false;
  bool check_empty_ = false;
  bool catchup_ = false;
  uint64_t stored_value_id_ = 0;

  EntryRecord local_entry_record_;
  EntryRecord peer_entry_record_;

  uint64_t max_committed_entry_ = 0;
  uint64_t max_chosen_entry_ = 0;

 public:
  PaxosCmd();
  PaxosCmd(uint64_t entity_id, uint64_t entry);
  virtual ~PaxosCmd() {}

  virtual std::string ToString() final;
  virtual bool ParseFromBuffer(const char* buffer, uint32_t len) final;
  virtual bool SerializeToBuffer(char* buffer, uint32_t len,
                                 uint32_t& real_len) final;
  virtual uint32_t SerializedByteSize() final;

  bool SwitchToLocalView(uint32_t local_acceptor_id);
  void RemoveValueInRecord();

  void SetChosen(bool value) { local_entry_record_.set_chosen(value); }

  CERTAIN_GET_SET(bool, plog_load);
  CERTAIN_GET_SET(bool, plog_range_load);
  CERTAIN_GET_SET(bool, plog_get_record);
  CERTAIN_GET_SET(bool, plog_set_record);
  CERTAIN_GET_SET(bool, plog_return_msg);
  CERTAIN_GET_SET(uint64_t, stored_value_id);

  CERTAIN_GET_SET(EntryRecord, local_entry_record);
  CERTAIN_GET_SET(EntryRecord, peer_entry_record);

  CERTAIN_GET_SET(uint64_t, max_committed_entry);
  CERTAIN_GET_SET(uint64_t, max_chosen_entry);
  CERTAIN_GET_SET(bool, check_empty);
  CERTAIN_GET_SET(bool, catchup);
};

class RangeCatchupCmd : public MsgBase {
 public:
  RangeCatchupCmd() : MsgBase(kCmdRangeCatchup) {}
  RangeCatchupCmd(uint64_t entity_id, uint64_t entry);
  virtual ~RangeCatchupCmd() {}

  CERTAIN_GET_SET(uint64_t, begin_entry);
  CERTAIN_GET_SET(uint64_t, end_entry);

  virtual std::string ToString() final;
  virtual bool ParseFromBuffer(const char* buffer, uint32_t len) final;
  virtual bool SerializeToBuffer(char* buffer, uint32_t len,
                                 uint32_t& real_len) final;
  virtual uint32_t SerializedByteSize() final;

  bool SwitchToLocalView(uint32_t local_acceptor_id);

 private:
  uint64_t begin_entry_ = 0;
  uint64_t end_entry_ = 0;
};

struct LibcoNotifyContext;
class ClientCmd : public CmdBase {
 public:
  ClientCmd(uint8_t cmd_id);
  virtual ~ClientCmd() {}

  CERTAIN_GET_SET(std::string, value);
  CERTAIN_GET_SET(std::vector<uint64_t>, uuids);
  CERTAIN_GET_SET(uint64_t, value_id);
  CERTAIN_GET_SET(uint64_t, timeout_msec);

  CERTAIN_GET_SET_PTR(LibcoNotifyContext*, context);

  virtual std::string ToString() override;

 private:
  uint64_t value_id_ = 0;
  uint64_t timeout_msec_ = 0;
  std::string value_;
  std::vector<uint64_t> uuids_;
  LibcoNotifyContext* context_ = nullptr;
};

class CmdFactory : public Singleton<CmdFactory> {
 public:
  int Init(Options* options) { return 0; }
  void Destroy() {}

  std::unique_ptr<MsgBase> CreateCmd(uint8_t cmd_id, const char* buffer,
                                     uint32_t len);
};

}  // namespace certain
