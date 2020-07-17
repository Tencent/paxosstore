#pragma once

#include "src/async_queue_mng.h"
#include "src/entry_info_mng.h"
#include "src/entry_state.h"
#include "src/msg_worker.h"
#include "src/wrapper.h"
#include "utils/header.h"
#include "utils/traffic_limiter.h"

namespace certain {

const uint32_t kMaxTimeoutMsec = 60 * 1000;

class EntityHelper {
 public:
  EntityHelper(Options* options, uint32_t worker_id, AsyncQueue* user_rsp_queue,
               Route* route)
      : options_(options),
        monitor_(options->monitor()),
        user_rsp_queue_(user_rsp_queue),
        entity_info_mng_(std::make_unique<EntityInfoMng>(options)),
        entry_info_mng_(std::make_unique<EntryInfoMng>(options, worker_id)),
        timer_(std::make_unique<ArrayTimer<EntityInfo>>(kMaxTimeoutMsec)),
        catchup_get_count_limiter_(std::make_unique<CountLimiter>()),
        catchup_sync_count_limiter_(std::make_unique<CountLimiter>()),
        route_(route) {
    uint64_t catchup_get_count =
        options_->catchup_max_get_per_second() / options_->entity_worker_num();
    catchup_get_count_limiter_->UpdateCount(catchup_get_count);

    uint64_t catchup_sync_count = options_->catchup_max_count_per_second() /
                                  options_->entity_worker_num();
    catchup_sync_count_limiter_->UpdateCount(catchup_sync_count);
  }

  ~EntityHelper() {}

  bool CheckIfHasWork();
  int HandleClientCmd(std::unique_ptr<ClientCmd>& client_cmd);
  int HandlePaxosCmd(std::unique_ptr<PaxosCmd>& pcmd);
  int HandlePlogRspCmd(std::unique_ptr<PaxosCmd>& pcmd);
  int HandleRecoverRspCmd(std::unique_ptr<PaxosCmd>& pcmd);

  uint64_t MemoryUsage() const;
  void DumpEntry(std::unique_ptr<ClientCmd> cmd);

 private:
  Options* options_;
  Monitor* monitor_;

  // entityworker(EntityHelper) -> entityworker(EntityWorker)
  AsyncQueue* user_rsp_queue_;
  std::list<std::unique_ptr<ClientCmd>> finished_list_;

  std::unique_ptr<EntityInfoMng> entity_info_mng_;
  std::unique_ptr<EntryInfoMng> entry_info_mng_;
  std::unique_ptr<ArrayTimer<EntityInfo>> timer_;
  std::unique_ptr<CountLimiter> catchup_get_count_limiter_;
  std::unique_ptr<CountLimiter> catchup_sync_count_limiter_;

  Route* route_;

  int HandleWriteCmd(EntryInfo* info);
  int HandleReadCmd(EntryInfo* info);

  void FinishClientCmd(std::unique_ptr<ClientCmd>& client_cmd, int result);
  void FinishClientCmd(EntityInfo* entity_info, int result);
  void HandleWaitingMsg(EntryInfo* info);

  int HandleGetFromPlog(std::unique_ptr<PaxosCmd>& pcmd);
  int HandleSetFromPlog(std::unique_ptr<PaxosCmd>& pcmd);
  int HandleLoadFromPlog(std::unique_ptr<PaxosCmd>& pcmd);

  int UpdateMachineByPaxosCmd(EntryInfo* info, std::unique_ptr<PaxosCmd>& pcmd);

  bool HandleIfLocalChosen(EntityInfo* entity_info,
                           std::unique_ptr<PaxosCmd>& pcmd);

  bool LoadEntryInfo(EntryInfo* info);
  bool LoadEntityInfo(EntityInfo* entity_info);
  bool StoreEntryInfo(EntryInfo* info);
  void ClearEntryInfo(EntryInfo* info);

  void RestoreValue(EntryInfo* info, std::unique_ptr<PaxosCmd>& pcmd);

  void IterateAndCatchup();

  // The moments to try catchup:
  // 1. Inside the IterateAndCatchup.
  // 2. It's found that the request's entry is not match the next one when
  // recieving users' requests. Probably max_cont_chosen_entry is less than
  // max_chosen_entry.
  // 3. Fast failed for read request.
  // 4. At the end of handling paxos messages from peers.
  // 5. At the end of handling getting from plog.
  void TryCatchup(EntityInfo* entity_info, int line = 0);

  void CatchupToPeer(EntryInfo* info);
  void UpdateCatchupTimestamp(EntryInfo* info);
  void TryRangeCatchup(EntityInfo* entity_info);

  int TriggerRecover(EntityInfo* entity_info);

  void Broadcast(EntryInfo* info, bool check_empty = false);
  void BroadcastOnAccept(EntryInfo* info);
  void BroadcastOnChosen(EntryInfo* info);
  void SyncToPeer(EntryInfo* info, uint32_t peer_acceptor_id,
                  bool check_empty = false, bool catchup = false);

  void UpdateByChosenEntry(EntryInfo* info);
  void UpdateByLoadFromPlog(EntityInfo* entity_info,
                            std::unique_ptr<PaxosCmd>& pcmd);

  int CreateEntryInfoIfMiss(EntityInfo* entity_info, uint64_t entry,
                            EntryInfo*& info);
  int CreateEntityInfoIfMiss(uint64_t entity_id, EntityInfo*& entity_info);
};

}  // namespace certain
