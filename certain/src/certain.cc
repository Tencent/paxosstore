#include "certain/certain.h"

#include "certain/errors.h"
#include "src/async_queue_mng.h"
#include "src/command.h"
#include "src/db_limited_worker.h"
#include "src/entity_info_mng.h"
#include "src/libco_notify_helper.h"
#include "src/wrapper.h"
#include "utils/co_lock.h"
#include "utils/uuid_mng.h"

namespace certain {
namespace {
auto wrapper = Wrapper::GetInstance();
}  // namespace

int Certain::Init(Options* options, Route* route_impl, Plog* plog_impl,
                  Db* db_impl) {
  return wrapper->Init(options, route_impl, plog_impl, db_impl);
}

Route* Certain::GetRoute() { return wrapper->GetRouteImpl(); }

Plog* Certain::GetPlog() { return wrapper->GetPlogImpl(); }

Db* Certain::GetDb() { return wrapper->GetDbImpl(); }

int Certain::Write(const CmdOptions& cmd_options, uint64_t entity_id,
                   uint64_t entry, const std::string& value,
                   const std::vector<uint64_t>& uuids) {
  TimeDelta time_delta;
  int ret = wrapper->Write(cmd_options, entity_id, entry, value, uuids);
  auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
  monitor->ReportWriteTimeCost(ret, time_delta.DeltaUsec());
  return ret;
}

int Certain::Read(const CmdOptions& cmd_options, uint64_t entity_id,
                  uint64_t entry) {
  TimeDelta time_delta;
  int ret = wrapper->Read(cmd_options, entity_id, entry);
  auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
  monitor->ReportReadTimeCost(ret, time_delta.DeltaUsec());
  return ret;
}

int Certain::Replay(const CmdOptions& cmd_options, uint64_t entity_id,
                    uint64_t* entry) {
  TimeDelta time_delta;
  int ret = wrapper->Replay(cmd_options, entity_id, entry);
  auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
  monitor->ReportReplayTimeCost(ret, time_delta.DeltaUsec());
  return ret;
}

int Certain::EvictEntity(uint64_t entity_id) {
  TimeDelta time_delta;
  int ret = wrapper->EvictEntity(entity_id);
  auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
  monitor->ReportEvictEntityTimeCost(ret, time_delta.DeltaUsec());
  return ret;
}

int Certain::GetWriteValue(uint64_t entity_id, uint64_t entry,
                           std::string* write_value) {
  TimeDelta time_delta;
  int ret = wrapper->GetWriteValue(entity_id, entry, write_value);
  auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
  monitor->ReportGetWriteValueTimeCost(ret, time_delta.DeltaUsec());
  return ret;
}

bool Certain::Exist(uint64_t entity_id, uint64_t uuid) {
  bool exist = UuidMng::GetInstance()->Exist(entity_id, uuid);
  auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
  if (exist) {
    monitor->ReportUuidExist();
  } else {
    monitor->ReportUuidNotExist();
  }
  return exist;
}

void Certain::Start() { wrapper->Start(); }

void Certain::Stop() {
  wrapper->set_exit_flag(true);
  wrapper->WaitExit();
  wrapper->Destroy();
}

bool Certain::Started() { return wrapper->Started(); }
bool Certain::Stopped() { return wrapper->Stopped(); }
void Certain::RunTick() { Tick::Run(); }

}  // namespace certain
