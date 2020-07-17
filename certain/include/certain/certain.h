#pragma once

#include <string>
#include <vector>

#include "certain/db.h"
#include "certain/log.h"
#include "certain/monitor.h"
#include "certain/options.h"
#include "certain/plog.h"
#include "certain/route.h"

namespace certain {

class Certain {
 public:
  static int Init(Options* options, Route* route_impl, Plog* plog_impl,
                  Db* db_impl);

  static Route* GetRoute();

  static Plog* GetPlog();

  static Db* GetDb();

  static int Write(const CmdOptions& options, uint64_t entity_id,
                   uint64_t entry, const std::string& value,
                   const std::vector<uint64_t>& uuids);

  static int Read(const CmdOptions& options, uint64_t entity_id,
                  uint64_t entry);

  static int Replay(const CmdOptions& options, uint64_t entity_id,
                    uint64_t* entry);

  static int EvictEntity(uint64_t entity_id);

  static int GetWriteValue(uint64_t entity_id, uint64_t entry,
                           std::string* write_value);

  static bool Exist(uint64_t entity_id, uint64_t uuid);

  static void Start();

  static void Stop();

  static bool Started();

  static bool Stopped();

  static void RunTick();
};

}  // namespace certain
