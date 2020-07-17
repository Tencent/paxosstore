#pragma once

#include "certain/monitor.h"

#ifdef OSS_ATTR_API
#include "iOssAttrApi.h"
inline void OssAttrIncInner(uint32_t id, uint32_t key, uint32_t val) {
  OssAttrInc(id, key, val);
}
#else
inline void OssAttrIncInner(uint32_t id, uint32_t key, uint32_t val) {}
#endif

class MonitorImpl : public certain::Monitor {
 public:
  MonitorImpl(uint32_t monitor_id1, uint32_t monitor_id2)
      : monitor_id1_(monitor_id1), monitor_id2_(monitor_id2) {}
  ~MonitorImpl() {}

  // For interface.
  virtual void ReportWriteTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id1_, 0, result, us);
  }
  virtual void ReportReadTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id1_, 5, result, us);
  }
  virtual void ReportReplayTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id1_, 10, result, us);
  }
  virtual void ReportEvictEntityTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id1_, 15, result, us);
  }
  virtual void ReportGetWriteValueTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id1_, 20, result, us);
  }
  virtual void ReportUuidExist() override {
    OssAttrIncInner(monitor_id1_, 40, 1);
  }
  virtual void ReportUuidNotExist() { OssAttrIncInner(monitor_id1_, 41, 1); }

  // For EntityWorker/CatchupWorker/RecoverWorker.
  virtual void ReportRecoverTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id1_, 25, result, us);
  }
  virtual void ReportWriteForRead() override {
    OssAttrIncInner(monitor_id1_, 42, 1);
  }
  virtual void ReportFastFail() override {
    OssAttrIncInner(monitor_id1_, 43, 1);
  }
  virtual void ReportCatchupGetLimit() override {
    OssAttrIncInner(monitor_id1_, 44, 1);
  }
  virtual void ReportCatchupSyncLimit() override {
    OssAttrIncInner(monitor_id1_, 45, 1);
  }
  virtual void ReportCatchupTotalFlowLimit() override {
    OssAttrIncInner(monitor_id1_, 46, 1);
  }
  virtual void ReportCatchupTotalCountLimit() override {
    OssAttrIncInner(monitor_id1_, 47, 1);
  }
  virtual void ReportCatchupTimes(uint32_t times) override {
    if (times <= 5) {
      OssAttrIncInner(monitor_id1_, 50 + times, 1);
    } else {
      OssAttrIncInner(monitor_id1_, 56, 1);
    }
  }
  virtual void ReportReadyToCatchup(uint32_t count) override {
    if (count <= 9) {
      OssAttrIncInner(monitor_id1_, 58 + count, 1);
    } else {
      OssAttrIncInner(monitor_id1_, 68, 1);
    }
  }
  virtual void ReportGetForCatchup() override {
    OssAttrIncInner(monitor_id1_, 72, 1);
  }
  virtual void ReportSyncForCatchup() override {
    OssAttrIncInner(monitor_id1_, 73, 1);
  }
  virtual void ReportAyncCommitForCatchup() override {
    OssAttrIncInner(monitor_id1_, 74, 1);
  }
  virtual void ReportEntityCreate() override {
    OssAttrIncInner(monitor_id1_, 75, 1);
  }
  virtual void ReportEntityDestroy() override {
    OssAttrIncInner(monitor_id1_, 76, 1);
  }
  virtual void ReportEvictEntitySucc() override {
    OssAttrIncInner(monitor_id1_, 77, 1);
  }
  virtual void ReportEvictEntityFail() override {
    OssAttrIncInner(monitor_id1_, 78, 1);
  }
  virtual void ReportEntityCountLimit() override {
    OssAttrIncInner(monitor_id1_, 79, 1);
  }
  virtual void ReportEntityMemoryLimit() override {
    OssAttrIncInner(monitor_id1_, 80, 1);
  }
  virtual void ReportEntryCountLimit() override {
    OssAttrIncInner(monitor_id1_, 81, 1);
  }
  virtual void ReportEntryMemoryLimit() override {
    OssAttrIncInner(monitor_id1_, 82, 1);
  }
  virtual void ReportChosenProposalNum(uint32_t num) {
    if (num < 10) {
      OssAttrIncInner(monitor_id1_, 89 + num, 1);
    } else {
      OssAttrIncInner(monitor_id1_, 100, 1);
    }
  }

  // For DbWorker/PlogWorker/DbLimitedWorker.
  virtual void ReportDbCommitTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 0, result, us);
  }
  virtual void ReportDbLimitedCommitTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 5, result, us);
  }
  virtual void ReportWrapperCommitTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 10, result, us);
  }
  virtual void ReportGetValueTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 15, result, us);
  }
  virtual void ReportSetValueTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 20, result, us);
  }
  virtual void ReportGetRecordTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 25, result, us);
  }
  virtual void ReportSetRecordTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 30, result, us);
  }
  virtual void ReportMultiSetRecordsTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 35, result, us);
  }
  virtual void ReportRangeGetRecordTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 40, result, us);
  }
  virtual void ReportGetStatusTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 45, result, us);
  }
  virtual void ReportLoadMaxEntryTimeCost(int result, uint64_t us) override {
    ReportTimeCost(monitor_id2_, 50, result, us);
  }
  virtual void ReportReadyToDbLimitedCommit(int num) override {
    OssAttrIncInner(monitor_id2_, 70, num);
  }
  virtual void ReportTotalRecordByMultiSet(int num) override {
    OssAttrIncInner(monitor_id2_, 71, num);
  }

  // For queue fail.
  virtual void ReportUserReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 80, 1);
  }
  virtual void ReportUserRspQueueFail() override {
    OssAttrIncInner(monitor_id2_, 81, 1);
  }
  virtual void ReportMsgReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 82, 1);
  }
  virtual void ReportEntityReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 83, 1);
  }
  virtual void ReportDbReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 84, 1);
  }
  virtual void ReportDbLimitedReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 85, 1);
  }
  virtual void ReportCatchupReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 86, 1);
  }
  virtual void ReportRecoverReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 87, 1);
  }
  virtual void ReportRecoverRspQueueFail() override {
    OssAttrIncInner(monitor_id2_, 88, 1);
  }
  virtual void ReportToolsReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 89, 1);
  }
  virtual void ReportPlogReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 90, 1);
  }
  virtual void ReportReadonlyReqQueueFail() override {
    OssAttrIncInner(monitor_id2_, 91, 1);
  }
  virtual void ReportPlogRspQueueFail() override {
    OssAttrIncInner(monitor_id2_, 92, 1);
  }

  virtual void ReportLowProbabilityError() override {
    OssAttrIncInner(monitor_id1_, 126, 1);
  }
  virtual void ReportFatalError() override {
    OssAttrIncInner(monitor_id1_, 127, 1);
  }

 private:
  uint32_t monitor_id1_;
  uint32_t monitor_id2_;

  void ReportTimeCost(uint32_t id, uint32_t key, int result, uint64_t us) {
    if (result != 0) {
      OssAttrIncInner(id, key + 4, 1);
      return;
    }
    if (us <= 10000) {
      OssAttrIncInner(id, key, 1);
    } else if (us <= 30000) {
      OssAttrIncInner(id, key + 1, 1);
    } else if (us <= 100000) {
      OssAttrIncInner(id, key + 2, 1);
    } else {
      OssAttrIncInner(id, key + 3, 1);
    }
  }
};
