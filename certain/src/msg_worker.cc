#include "src/msg_worker.h"

#include "network/msg_channel.h"
#include "src/command.h"
#include "src/conn_mng.h"
#include "src/msg_channel_helper.h"
#include "src/msg_serialize.h"
#include "utils/log.h"
#include "utils/memory.h"

namespace certain {

int MsgWorker::HandleRead(FdObj* fd_obj) {
  int ret;
  MsgChannel* channel = dynamic_cast<MsgChannel*>(fd_obj);
  channel->ReadMore(read_buf_.get(), kReadBufferSize);

  if (channel->broken()) {
    channel_helper_.RemoveMsgChannel(channel);
    return -1;
  }

  // It may be still readable when read_buf_ is full.
  if (channel->readable()) {
    ret = channel_helper_.RefreshMsgChannel(channel);
    CERTAIN_LOG_FATAL("RemoveMsgChannel channel: %s ret %d",
                      channel->ToString().c_str(), ret);
  }
  return 0;
}

void MsgWorker::OnMessage(uint8_t msg_id, const char* buffer, uint32_t len) {
  auto cmd = CmdFactory::GetInstance()->CreateCmd(msg_id, buffer, len);
  assert(cmd.get() != nullptr);
  auto queue = queue_mng_->GetEntityReqQueueByEntityId(cmd->entity_id());
  int ret = queue->PushByMultiThread(&cmd);
  if (ret != 0) {
    monitor_->ReportEntityReqQueueFail();
    CERTAIN_LOG_ERROR("entity req queue failed: %s", cmd->ToString().c_str());
  }
}

int MsgWorker::HandleWrite(FdObj* fd_obj) {
  MsgChannel* ch = dynamic_cast<MsgChannel*>(fd_obj);
  ch->FlushBuffer();

  if (ch->broken()) {
    channel_helper_.RemoveMsgChannel(ch);
    return -1;
  }

  // still writable
  if (ch->writable()) {
    channel_helper_.AddWritableChannel(ch);
  }

  return 0;
}

void MsgWorker::ServeNewConnect() {
  while (true) {
    auto socket = ConnMng::GetInstance()->TakeByMultiThread(worker_id_);
    if (socket == nullptr) {
      break;
    }
    auto channel = std::make_unique<MsgChannel>(std::move(socket), this,
                                                &rbuf_shared_limiter_,
                                                &wbuf_shared_limiter_, this);
    auto raw_pointer = channel.get();
    int ret = channel_helper_.AddMsgChannel(channel);
    if (ret != 0) {
      CERTAIN_LOG_ERROR("add msg failed: %s", raw_pointer->ToString().c_str());
      continue;
    }
    CERTAIN_LOG_ZERO("worker_id %u serve channel: %s", worker_id_,
                     raw_pointer->ToString().c_str());
  }
}

MsgChannel* MsgWorker::GetConnect(uint64_t peer_addr_id) {
  MsgChannel* exist = channel_helper_.GetMsgChannel(peer_addr_id);
  if (exist != nullptr) {
    return exist;
  }

  auto socket = std::make_unique<TcpSocket>();
  int ret = socket->InitSocket();
  if (ret != 0) {
    CERTAIN_LOG_ERROR("init socket failed %d", ret);
    return nullptr;
  }

  InetAddr peer_addr(peer_addr_id);
  ret = socket->Connect(peer_addr);
  if (ret != 0 && ret != kNetWorkInProgress) {
    CERTAIN_LOG_ERROR("connect socket failed %d", ret);
    return nullptr;
  }
  CERTAIN_LOG_INFO("socket: %s", socket->ToString().c_str());
  auto channel = std::make_unique<MsgChannel>(std::move(socket), this,
                                              &rbuf_shared_limiter_,
                                              &wbuf_shared_limiter_, this);
  auto raw_pointer = channel.get();
  ret = channel_helper_.AddMsgChannel(channel);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("add msg failed: %s", raw_pointer->ToString().c_str());
  }

  CERTAIN_LOG_INFO("add channel: %s", raw_pointer->ToString().c_str());
  return raw_pointer;
}

void MsgWorker::ConsumeMsgReqQueue() {
  auto queue = queue_mng_->GetMsgReqQueueByIdx(worker_id_);
  std::unique_ptr<PaxosCmd> cmd;
  while (queue->PopByOneThread(&cmd) == 0) {
    assert(cmd.get() != nullptr);
    auto entity_id = cmd->entity_id();

    Route* route = Wrapper::GetInstance()->GetRouteImpl();
    uint64_t peer_addr_id = 0;
    auto peer_acceptor_id = cmd->peer_acceptor_id();
    int ret =
        route->GetServerAddrId(entity_id, peer_acceptor_id, &peer_addr_id);
    if (ret != 0) {
      CERTAIN_LOG_ERROR("get server addr failed %d", ret);
    }
    MsgChannel* channel = GetConnect(peer_addr_id);

    uint32_t original_size = cmd->SerializedByteSize();
    cmd->RemoveValueInRecord();
    uint32_t removed_size = cmd->SerializedByteSize();
    CERTAIN_LOG_DEBUG("cmd: %s original_size %u removed_size %u",
                      cmd->ToString().c_str(), original_size, removed_size);
    MsgSerialize obj(cmd.get());
    SerializeCallBackBase* arr = &obj;
    bool ok = channel->Write(&arr, 1);
    if (!ok) {
      CERTAIN_LOG_ERROR("channel write failed: %s",
                        channel->ToString().c_str());
    }
  }
  channel_helper_.FlushMsgChannel();
}

void MsgWorker::GoAndDeleteIfFailed(std::unique_ptr<PaxosCmd> pcmd) {
  auto queue_mng = AsyncQueueMng::GetInstance();
  auto queue = queue_mng->GetMsgReqQueueByEntityId(pcmd->entity_id());
  int ret = queue->PushByMultiThread(&pcmd);
  if (ret != 0) {
    auto monitor = Wrapper::GetInstance()->GetMonitorImpl();
    monitor->ReportMsgReqQueueFail();
    CERTAIN_LOG_ERROR("msg req queue faild: %s", pcmd->ToString().c_str());
  }
}

void MsgWorker::Run() {
  while (!ThreadBase::exit_flag()) {
    ServeNewConnect();
    ConsumeMsgReqQueue();
    poller_.RunOnce(1);
  }
}

}  // namespace certain
