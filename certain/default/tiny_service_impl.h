#pragma once

#include "proto/tiny_rpc.pb.h"

class TinyServiceImpl : public certain::TinyRpcService {
  void SnapshotRecover(google::protobuf::RpcController* controller,
                       const certain::SnapshotRecoverReq* req,
                       certain::SnapshotRecoverRsp* rsp,
                       google::protobuf::Closure* done) override;

  void Write(google::protobuf::RpcController* controller,
             const certain::WriteReq* req, certain::WriteRsp* rsp,
             google::protobuf::Closure* done) override;

  void Read(google::protobuf::RpcController* controller,
            const certain::ReadReq* req, certain::ReadRsp* rsp,
            google::protobuf::Closure* done) override;

  void AppendString(google::protobuf::RpcController* controller,
                    const certain::AppendStringReq* req,
                    certain::AppendStringRsp* rsp,
                    google::protobuf::Closure* done) override;

  void GetStringStatus(google::protobuf::RpcController* controller,
                       const certain::GetStringStatusReq* req,
                       certain::GetStringStatusRsp* rsp,
                       google::protobuf::Closure* done) override;
};
