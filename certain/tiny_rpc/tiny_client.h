#pragma once

#include <string>

#include "proto/tiny_rpc.pb.h"
#include "tiny_rpc/tiny_rpc.h"

namespace certain {

class TinyController : public google::protobuf::RpcController {
 public:
  int RetCode() const { return ret_; }
  void SetRetCode(int ret) { ret_ = ret; }
  void Reset() override { ret_ = 0; }

  // not use in tiny rpc
  bool Failed() const override { return RetCode() != 0; }
  void SetFailed(const std::string& reason) override { ret_ = kTinyRpcNotImpl; }
  std::string ErrorText() const override { return ""; }

  bool IsCanceled() const override { return false; }
  void StartCancel() override {}
  void NotifyOnCancel(::google::protobuf::Closure* callback) override {}

 private:
  int ret_ = 0;
};

class TinyChannel : public google::protobuf::RpcChannel {
 public:
  TinyChannel(const std::string& peer_addr) : peer_addr_(peer_addr) {}

  void CallMethod(const google::protobuf::MethodDescriptor* method,
                  google::protobuf::RpcController* controller,
                  const google::protobuf::Message* request,
                  google::protobuf::Message* response,
                  google::protobuf::Closure* done) override;

 private:
  std::string peer_addr_;
};

class TinyClient : public TinyRpcService_Stub {
 public:
  TinyClient(std::string peer_addr)
      : TinyRpcService_Stub(&channel_), channel_(peer_addr) {}

 private:
  TinyChannel channel_;
};

}  // namespace certain
