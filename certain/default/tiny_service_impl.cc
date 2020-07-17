#include "default/tiny_service_impl.h"

#include "certain/certain.h"
#include "default/db_impl.h"
#include "tiny_rpc/tiny_client.h"
#include "utils/memory.h"

void TinyServiceImpl::SnapshotRecover(
    google::protobuf::RpcController* controller_base,
    const certain::SnapshotRecoverReq* req, certain::SnapshotRecoverRsp* rsp,
    google::protobuf::Closure* done) {
  auto db_impl = dynamic_cast<DbImpl*>(certain::Certain::GetDb());
  uint64_t entity_id = req->entity_id();
  auto& db_info = db_impl->Shard(entity_id);
  uint64_t entry = 0;
  uint32_t crc32 = 0;
  certain::Db::RecoverFlag flag;
  int ret = db_info.Get(entity_id, &entry, &crc32, &flag);
  assert(ret == 0 || ret == certain::kImplDbNotFound);
  rsp->set_max_apply_entry(entry);
  rsp->set_data(
      std::string(reinterpret_cast<const char*>(&crc32), sizeof(crc32)));
}

void TinyServiceImpl::Write(google::protobuf::RpcController* controller_base,
                            const certain::WriteReq* req,
                            certain::WriteRsp* rsp,
                            google::protobuf::Closure* done) {
  auto& controller = *static_cast<certain::TinyController*>(controller_base);

  uint64_t entity_id = req->entity_id();
  uint64_t entry = req->entry();
  std::string value = req->value();
  std::vector<uint64_t> uuids{req->uuids().begin(), req->uuids().end()};

  int ret = certain::Certain::Write(certain::CmdOptions::Default(), entity_id,
                                    entry, value, uuids);
  CERTAIN_LOG_INFO("E(%lu, %lu) value.sz %lu uuids.sz %lu ret %d", entity_id,
                   entry, value.size(), uuids.size(), ret);
  controller.SetRetCode(ret);
}

void TinyServiceImpl::Read(google::protobuf::RpcController* controller_base,
                           const certain::ReadReq* req, certain::ReadRsp* rsp,
                           google::protobuf::Closure* done) {
  auto& controller = *static_cast<certain::TinyController*>(controller_base);

  uint64_t entity_id = req->entity_id();
  uint64_t entry = req->entry();
  int ret =
      certain::Certain::Read(certain::CmdOptions::Default(), entity_id, entry);
  CERTAIN_LOG_INFO("E(%lu, %lu) ret %d", entity_id, entry, ret);
  controller.SetRetCode(ret);
}

void TinyServiceImpl::AppendString(
    google::protobuf::RpcController* controller_base,
    const certain::AppendStringReq* req, certain::AppendStringRsp* rsp,
    google::protobuf::Closure* done) {
  auto& controller = *static_cast<certain::TinyController*>(controller_base);

  uint64_t entity_id = req->entity_id();
  std::string value = req->value();
  std::vector<uint64_t> uuids;
  if (req->has_uuid()) {
    uuids.push_back(req->uuid());
  }
  CERTAIN_LOG_INFO("entity_id %lu value.sz %lu has_uuid %u uuid %lu", entity_id,
                   value.size(), req->has_uuid(), req->uuid());

  uint64_t entry = 0;
  int ret = certain::Certain::Replay(certain::CmdOptions::Default(), entity_id,
                                     &entry);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("Replay entity_id %lu ret %d", entity_id, ret);
    return controller.SetRetCode(ret);
  }

  rsp->set_write_entry(++entry);
  ret = certain::Certain::Write(certain::CmdOptions::Default(), entity_id,
                                entry, value, uuids);
  if (ret != 0) {
    CERTAIN_LOG_INFO("E(%lu, %lu) value.sz %lu uuids.sz %lu ret %d", entity_id,
                     entry, value.size(), uuids.size(), ret);
    return controller.SetRetCode(ret);
  }

  uint64_t current_entry = 0;
  uint32_t current_crc32 = 0;
  certain::Db::RecoverFlag flag = certain::Db::kNormal;
  auto db_impl = dynamic_cast<DbImpl*>(certain::Certain::GetDb());
  auto& db_info = db_impl->Shard(entity_id);
  ret = db_info.Get(entity_id, &current_entry, &current_crc32, &flag);
  assert(ret == 0 || ret == certain::kImplDbNotFound);
  rsp->set_current_entry(current_entry);
  rsp->set_current_crc32(current_crc32);
}

void TinyServiceImpl::GetStringStatus(
    google::protobuf::RpcController* controller_base,
    const certain::GetStringStatusReq* req, certain::GetStringStatusRsp* rsp,
    google::protobuf::Closure* done) {
  auto& controller = *static_cast<certain::TinyController*>(controller_base);

  uint64_t entity_id = req->entity_id();
  CERTAIN_LOG_INFO("entity_id %lu", entity_id);

  uint64_t entry = 0;
  int ret = certain::Certain::Replay(certain::CmdOptions::Default(), entity_id,
                                     &entry);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("Replay entity_id %lu ret %d", entity_id, ret);
    return controller.SetRetCode(ret);
  }

  rsp->set_read_entry(++entry);
  ret =
      certain::Certain::Read(certain::CmdOptions::Default(), entity_id, entry);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("E(%lu, %lu) ret %d", entity_id, entry, ret);
    return controller.SetRetCode(ret);
  }

  uint64_t current_entry = 0;
  uint32_t current_crc32 = 0;
  certain::Db::RecoverFlag flag = certain::Db::kNormal;
  auto db_impl = dynamic_cast<DbImpl*>(certain::Certain::GetDb());
  auto& db_info = db_impl->Shard(entity_id);
  ret = db_info.Get(entity_id, &current_entry, &current_crc32, &flag);
  assert(ret == 0 || ret == certain::kImplDbNotFound);
  rsp->set_current_entry(current_entry);
  rsp->set_current_crc32(current_crc32);
}
