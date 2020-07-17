#pragma once

namespace certain {

enum RetCodeErrors {
  kRetCodeOk = 0,

  kRetCodeNotFound = -3000,
  kRetCodeClientCmdConflict = -3001,
  kRetCodeLocalAcceptorIdErr = -3002,
  kRetCodeTimeout = -3003,
  kRetCodeInited = -3004,
  kRetCodeNoIdleNotifier = -3005,
  kRetCodeEntryNotMatch = -3006,
  kRetCodeEntryUncertain = -3007,
  kRetCodeParameterErr = -3008,
  kRetCodeEntryLimited = -3009,
  kRetCodeLoadEntryFailed = -3010,
  kRetCodeEntityLimited = -3011,
  kRetCodeLoadEntityFailed = -3012,
  kRetCodeStoreEntryFailed = -3013,
  kRetCodeStatePromiseErr = -3014,
  kRetCodeStateAcceptErr = -3015,
  kRetCodeStateAcceptFailed = -3016,
  kRetCodeEntityLoading = -3017,
  kRetCodeWaitBroadcast = -3018,
  kRetCodeFastFailed = -3019,
  kRetCodeReadFailed = -3020,
  kRetCodeNotChosen = -3021,
  kRetCodeRecoverPending = -3022,
  kRetCodeCatchupPending = -3023,
  kRetCodeReplayPending = -3024,
  kRetCodeEntryCleanup = -3025,

  kRetCodeInvalidRecord = -3100,
  kRetCodeInvalidAcceptorId = -3101,
  kRetCodeInvalidEntryState = -3102,

  kRetCodeUnknown = -3999,
};

enum NetWorkErrors {
  kNetWorkError = -2000,
  kNetWorkWouldBlock = -2001,
  kNetWorkInProgress = -2002,
};

enum UtilsErrors {
  kUtilsInvalidArgs = -1000,
  kUtilsQueueFull = -1001,
  kUtilsQueueConflict = -1002,
  kUtilsQueueEmpty = -1003,
  kUtilsQueueFail = -1004,
};

enum ImplErrors {
  kImplPlogNotFound = -4000,
  kImplPlogGetErr = -4001,
  kImplPlogSetErr = -4002,
  kImplDbNotFound = -4003,
  kImplDbGetErr = -4004,
  kImplDbSetErr = -4005,
  kImplDbNotMatch = -4006,
  kImplDbRecoverFail = -4007,
  kImplUnknown = -4999,
};

enum TinyRpcErrors {
  kTinyRpcNotImpl = -5000,
};

}  // namespace certain
