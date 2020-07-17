#pragma once

#include "certain/db.h"
#include "certain/log.h"
#include "certain/monitor.h"
#include "certain/options.h"
#include "certain/plog.h"
#include "certain/route.h"
#include "utils/singleton.h"
#include "utils/thread.h"

namespace certain {

class ClientCmd;
class Wrapper : public Singleton<Wrapper>, public ThreadBase {
 public:
  Wrapper() : ThreadBase("wrapper") {}

  int Init(Options* options, Route* route_impl, Plog* plog_impl, Db* db_impl);
  void Destroy();

  Options* GetOptions() { return options_; }
  Route* GetRouteImpl() { return route_impl_; }
  Plog* GetPlogImpl() { return plog_impl_; }
  Db* GetDbImpl() { return db_impl_; }
  Monitor* GetMonitorImpl() { return monitor_impl_; }

  int Write(const CmdOptions& options, uint64_t entity_id, uint64_t entry,
            const std::string& value, const std::vector<uint64_t>& uuids);

  int Read(const CmdOptions& options, uint64_t entity_id, uint64_t entry);

  int Replay(const CmdOptions& options, uint64_t entity_id, uint64_t* entry);

  int EvictEntity(uint64_t entity_id);

  int GetWriteValue(uint64_t entity_id, uint64_t entry,
                    std::string* write_value);

  virtual void Run() override;
  virtual void WaitExit() override;

  bool Started() const final { return started_; }

 private:
  // The following methods Called in Run().
  void StartWorkers();
  void StopWorkers();

  // The following methods Called in Init().
  int InitManagers();
  int InitWorkers();

  // The following methods Called in Destroy().
  void DestroyManagers();
  void DestroyWorkers();

 private:
  Options* options_;
  Route* route_impl_;
  Plog* plog_impl_;
  Db* db_impl_;
  Monitor* monitor_impl_ = &Monitor::Instance();
  bool started_ = false;

  std::vector<std::unique_ptr<ThreadBase>> workers_;
};

}  // namespace certain
