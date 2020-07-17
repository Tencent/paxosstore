#pragma once

#include <iostream>

namespace certain {

class Monitor {
 public:
  // For interface.
  virtual void ReportWriteTimeCost(int result, uint64_t us) {}
  virtual void ReportReadTimeCost(int result, uint64_t us) {}
  virtual void ReportReplayTimeCost(int result, uint64_t us) {}
  virtual void ReportEvictEntityTimeCost(int result, uint64_t us) {}
  virtual void ReportGetWriteValueTimeCost(int result, uint64_t us) {}
  virtual void ReportUuidExist() {}
  virtual void ReportUuidNotExist() {}

  // For EntityWorker/CatchupWorker/RecoverWorker.
  virtual void ReportRecoverTimeCost(int result, uint64_t us) {}
  virtual void ReportWriteForRead() {}
  virtual void ReportFastFail() {}
  virtual void ReportCatchupGetLimit() {}
  virtual void ReportCatchupSyncLimit() {}
  virtual void ReportCatchupTotalFlowLimit() {}
  virtual void ReportCatchupTotalCountLimit() {}
  virtual void ReportCatchupTimes(uint32_t times) {}
  virtual void ReportReadyToCatchup(uint32_t count) {}
  virtual void ReportGetForCatchup() {}
  virtual void ReportSyncForCatchup() {}
  virtual void ReportAyncCommitForCatchup() {}
  virtual void ReportEntityCreate() {}
  virtual void ReportEntityDestroy() {}
  virtual void ReportEvictEntitySucc() {}
  virtual void ReportEvictEntityFail() {}
  virtual void ReportEntityCountLimit() {}
  virtual void ReportEntityMemoryLimit() {}
  virtual void ReportEntryCountLimit() {}
  virtual void ReportEntryMemoryLimit() {}
  virtual void ReportChosenProposalNum(uint32_t num) {}

  // For DbWorker/PlogWorker/DbLimitedWorker.
  virtual void ReportDbCommitTimeCost(int result, uint64_t us) {}
  virtual void ReportDbLimitedCommitTimeCost(int result, uint64_t us) {}
  virtual void ReportWrapperCommitTimeCost(int result, uint64_t us) {}
  virtual void ReportGetValueTimeCost(int result, uint64_t us) {}
  virtual void ReportSetValueTimeCost(int result, uint64_t us) {}
  virtual void ReportGetRecordTimeCost(int result, uint64_t us) {}
  virtual void ReportSetRecordTimeCost(int result, uint64_t us) {}
  virtual void ReportMultiSetRecordsTimeCost(int result, uint64_t us) {}
  virtual void ReportRangeGetRecordTimeCost(int result, uint64_t us) {}
  virtual void ReportLoadMaxEntryTimeCost(int result, uint64_t us) {}
  virtual void ReportGetStatusTimeCost(int result, uint64_t us) {}
  virtual void ReportReadyToDbLimitedCommit(int num) {}
  virtual void ReportTotalRecordByMultiSet(int num) {}

  // For queue fail.
  virtual void ReportUserReqQueueFail() {}
  virtual void ReportUserRspQueueFail() {}
  virtual void ReportMsgReqQueueFail() {}
  virtual void ReportEntityReqQueueFail() {}
  virtual void ReportDbReqQueueFail() {}
  virtual void ReportDbLimitedReqQueueFail() {}
  virtual void ReportCatchupReqQueueFail() {}
  virtual void ReportRecoverReqQueueFail() {}
  virtual void ReportRecoverRspQueueFail() {}
  virtual void ReportToolsReqQueueFail() {}
  virtual void ReportPlogReqQueueFail() {}
  virtual void ReportReadonlyReqQueueFail() {}
  virtual void ReportPlogRspQueueFail() {}
  virtual void ReportContextQueueFail() {}

  virtual void ReportLowProbabilityError() {}
  virtual void ReportFatalError() {}

  static Monitor& Instance() {
    static Monitor monitor;
    return monitor;
  }
};

}  // namespace certain
