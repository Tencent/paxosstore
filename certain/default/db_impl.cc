#include "db_impl.h"

#include "certain/certain.h"
#include "tiny_rpc/tiny_client.h"

namespace {

class DbDumper : public certain::ThreadBase {
 public:
  DbImpl* db = nullptr;

  void Run() final {
    system("mkdir -p ./test_db.o");
    db->FromFile("./test_db.o/mem_db.txt");
    while (exit_flag() == false) {
      for (int i = 0; i < 10 && exit_flag() == false; ++i) {
        poll(nullptr, 0, 1000);
      }
      db->ToFile("./test_db.o/mem_db.txt");
    }
  }
};

static DbDumper g_db_dumper;

}  // namespace

DbImpl::DbImpl(uint32_t bucket_num) : buckets_(bucket_num) {
  // start dump thread
  g_db_dumper.db = this;
  g_db_dumper.Start();
}

DbImpl::~DbImpl() {
  // stop dump thread
  g_db_dumper.set_exit_flag(true);
  g_db_dumper.WaitExit();
}

int DbImpl::SnapshotRecover(uint64_t entity_id, uint32_t start_acceptor_id,
                            uint64_t* max_committed_entry) {
  // Step 1: Set flag
  {
    certain::DbEntityLock lock(this, entity_id);
    int ret = Shard(entity_id).Set(entity_id, -1u, 0, certain::Db::kRecover);
    assert(ret == 0);
  }

  // Step 2: Send RPC
  auto route = certain::Certain::GetRoute();

  // TODO: tune peer address calc
  uint32_t local_id;
  int ret = route->GetLocalAcceptorId(entity_id, &local_id);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("GetLocalAddrId Fail with %d", ret);
    return ret;
  }

  if (start_acceptor_id == UINT32_MAX) {
    start_acceptor_id = local_id + 1;
  }
  for (uint32_t i = 0; i < 3; ++i) {
    uint32_t acceptor_id = (i + start_acceptor_id) % 3;
    if (acceptor_id == local_id) {
      continue;
    }
    uint64_t addr_id;
    int ret = route->GetServerAddrId(entity_id, acceptor_id, &addr_id);
    if (ret != 0) {
      CERTAIN_LOG_ERROR("GetServerAddrId[%u] Fail with %d", acceptor_id, ret);
      return ret;
    }
    addr_id += 1000;  // port offset
    std::string peer_addr = certain::InetAddr(addr_id).ToString();

    certain::TinyClient client(peer_addr);
    certain::TinyController controller;
    certain::SnapshotRecoverReq req;
    req.set_entity_id(entity_id);
    certain::SnapshotRecoverRsp resp;
    client.SnapshotRecover(&controller, &req, &resp, nullptr);
    if (controller.Failed()) {
      continue;
    }

    *max_committed_entry = resp.max_apply_entry();
    assert(resp.data().size() == 4);
    uint32_t crc32 = *reinterpret_cast<const uint32_t*>(resp.data().data());

    // Step 3: Restore data
    {
      certain::DbEntityLock lock(this, entity_id);
      int ret = Shard(entity_id).Set(entity_id, resp.max_apply_entry(), crc32,
                                     certain::Db::kNormal);
      assert(ret == 0);
    }
    return 0;
  }
  return certain::kImplDbRecoverFail;
}

void DbImpl::LockEntity(uint64_t entity_id) { lock_.Lock(entity_id); }

void DbImpl::UnlockEntity(uint64_t entity_id) { lock_.Unlock(entity_id); }
