
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <poll.h>
#include <cassert>
#include "core/paxos.pb.h"
#include "core/pins_wrapper.h"
#include "core/plog_wrapper.h"
#include "core/plog_helper.h"
#include "core/err_code.h"
#include "cutils/log_utils.h"
#include "cutils/mem_utils.h"
#include "cutils/time_utils.h"
#include "cutils/id_utils.h"
#include "cutils/log_utils.h"
#include "msg_svr/msg_helper.h"
#include "dbcomm/db_comm.h"
#include "dbcomm/hashlock.h"
#include "db_helper.h"
#include "db_impl.h"


namespace {

void AssertCheck(
        const paxos::PaxosLogHeader& header, 
        const paxos::PaxosLog& plog)
{
    assert(header.max_index() == paxos::get_max_index(plog));
    assert(header.chosen_index() == paxos::get_chosen_index(plog));
    assert(header.reqid() == paxos::get_chosen_reqid(plog));

    auto chosen_ins = paxos::get_chosen_ins(plog);
    if (nullptr == chosen_ins || 0 == chosen_ins->index()) {
        assert(0 == header.version());
        return ;
    }

    assert(nullptr != chosen_ins);
    assert(chosen_ins->has_accepted_value());
    paxos::DBData data;
    assert(data.ParseFromString(chosen_ins->accepted_value().data()));
    assert(data.version() == header.version());
    return ;
}

void AssertCheckValue(
        const std::string& key, 
        uint64_t index, 
        uint64_t reqid, 
        const std::string& value)
{
    // TODO
}


int packRawValue(
        paxos::PaxosLogHeader& header, 
        const std::string& user_value, 
        std::string& raw_value)
{
    assert(header.chosen_index()+1 == header.max_index() || 
            header.chosen_index() == header.max_index());

    paxos::DBData data;
    data.set_version(header.version() + 1);
    data.set_flag(0);
    data.set_value(user_value);
    data.set_timestamp(time(nullptr));
    if (false == data.SerializeToString(&raw_value)) {
        return SERIALIZE_DBDATA_ERR;
    }

    assert(false == raw_value.empty());
    return 0;
}

int WaitForNotify(int iNotifyFD, uint32_t iTimeOutMS)
{
	// TODO: add perid check redo ??
	assert(0 <= iNotifyFD);
	uint64_t llNextBeginTime = dbcomm::GetTickMS();
	uint32_t iRemainTimeoutMS = iTimeOutMS;

	struct pollfd tEvent = {0};
	tEvent.fd = iNotifyFD;
	tEvent.events = POLLIN | POLLERR | POLLHUP;
	while (true)
	{
		errno = 0;
		int ret = poll(&tEvent, 1, iRemainTimeoutMS); // 1ms timeout
		if (0 >= ret)
		{
			if (-1 == ret)
			{
				if (EINTR == errno)
				{
					uint64_t llPassTime = 
                        dbcomm::GetTickMS() - llNextBeginTime;
					assert(0 <= llPassTime);
					if (llPassTime < static_cast<
                            uint64_t>(iRemainTimeoutMS))
					{
						uint32_t iPrevRemainTimeMS = iRemainTimeoutMS;
						iRemainTimeoutMS -= llPassTime;
						assert(iRemainTimeoutMS <= iPrevRemainTimeMS);
						llNextBeginTime = dbcomm::GetTickMS();
						continue;
					}
				}
			}

			// else
			iRemainTimeoutMS = 0;
			logerr("POLL ret %d iNotifyFD %d iTimeOutMS %u", 
					ret, iNotifyFD, iTimeOutMS);
			break;
		}

		assert(0 < ret); // succ: may cause by error: 
		return 0;
	}

	return -1; // TIMEOUT
}


int updateHeader(
        const paxos::PaxosLog& plog, 
        paxos::PaxosLogHeader& header)
{
    assert(paxos::is_slim(plog));
    header.set_max_index(paxos::get_max_index(plog));
    if (header.chosen_index() != paxos::get_chosen_index(plog)) {
        header.set_chosen_index(paxos::get_chosen_index(plog));
        if (0 == header.chosen_index()) {
            header.set_reqid(0);
            header.set_version(0);
            return 0;
        }

        assert(0 < header.chosen_index());
        auto chosen_ins = paxos::get_chosen_ins(plog);
        assert(nullptr != chosen_ins);
        header.set_reqid(chosen_ins->accepted_value().reqid());
        paxos::DBData data;
        assert(data.ParseFromString(chosen_ins->accepted_value().data()));
        header.set_version(data.version());
    }

    return 0;
}

std::unique_ptr<paxos::Message> 
BuildACatchUpMsg(const std::string& key, uint8_t selfid)
{
    assert(0 < selfid && 3 >= selfid);
    auto msg = cutils::make_unique<paxos::Message>();
    assert(nullptr != msg);

    msg->set_type(paxos::MessageType::GET_CHOSEN);
    msg->set_key(key);
    msg->set_index(0);
    msg->set_proposed_num(0);
    msg->set_from(selfid);
    msg->set_to(0);
    return msg;
}

} // namespace 


namespace paxoskv {


DBImpl::DBImpl(
        uint16_t member_id, 
        Option& option)
    : member_id_(member_id)
    , option_(option)
{
    // TODO
}


DBImpl::~DBImpl() = default;

int DBImpl::Init(
        paxos::SmartPaxosMsgSender* msg_sender)
{
    assert(nullptr != msg_sender);
    assert(nullptr == msg_sender_);

    msg_sender_ = msg_sender;

    assert(nullptr == localout_helper_);
    localout_helper_ = 
        cutils::make_unique<clsGetLocalOutHelper>(
                option_.lo_max_track_size, option_.lo_hold_interval_ms);
    assert(nullptr != localout_helper_);

    assert(nullptr == idgen_);
    assert(0 < member_id_);
    idgen_ = cutils::make_unique<
        cutils::IDGenerator>(member_id_, cutils::get_curr_ms());
    assert(nullptr != idgen_);

    assert(nullptr == hashbase_lock_);
    hashbase_lock_ = cutils::make_unique<dbcomm::HashBaseLock>();
    assert(nullptr != hashbase_lock_);
    std::string db_lock_path = option_.db_path + "/lock/db.lock";
    auto ret = hashbase_lock_->Init(
            db_lock_path.c_str(), option_.db_lock_size);
    if (0 != ret) {
        logerr("HashBaseLock::Init ret %d", ret);
        return ret;
    }

    alive_state_cache_ = cutils::make_unique<
        PInsAliveStateTimeoutCache>(
                option_.ascache_timeout_entries_size);
    assert(nullptr != alive_state_cache_);
    return 0;
}


int DBImpl::PostPaxosMsgNoLock(
        const paxos::Message& req_msg, 
        std::unique_ptr<paxos::Message>& rsp_msg)
{
    paxos::PaxosLogHeader header;
    paxos::PaxosLog plog;

    auto ret = option_.pfn_read(req_msg.key(), header, plog);
    if (0 != ret) {
        logerr("pfn_read ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    AssertCheck(header, plog);

    std::shared_ptr<paxos::PInsAliveState> alive_state = nullptr;
    uint64_t conflict_index = 0;
    std::tie(conflict_index, alive_state) = 
        alive_state_cache_->MoreGet(req_msg.key(), req_msg.index());
    if (nullptr != alive_state) {
        assert(0 == conflict_index);
        assert(alive_state->GetKey() == req_msg.key());
    }

    paxos::PLogWrapper wrapper(
            req_msg.to(), member_id_, 
            req_msg.key(), alive_state.get(), plog);
    std::tie(ret, rsp_msg) = wrapper.Step(req_msg);
    if (0 != ret) {
        logerr("Step req_msg type %d ret %d from %u to %u", 
                req_msg.type(), ret, req_msg.from(), req_msg.to());
        return ret;
    }

    if (wrapper.NeedDiskWrite()) {
        // update header;
        assert(0 == updateHeader(plog, header));
    }

    assert(0 == ret);
    ret = DoWriteNoLock(wrapper, header, plog);
    if (0 != ret) {
        rsp_msg = nullptr;
        logerr("DoWriteNoLock ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    AssertCheck(header, plog);
    if (nullptr != alive_state && alive_state->IsChosen()) {
        alive_state->SendNotify();
        alive_state_cache_->Destroy(req_msg.key(), req_msg.index());
    }

    if (0 != conflict_index && conflict_index < header.max_index()) {
        auto outdate_state = 
            alive_state_cache_->Get(req_msg.key(), conflict_index);
        if (nullptr != outdate_state) {
            outdate_state->SendNotify();
            alive_state_cache_->Destroy(req_msg.key(), conflict_index);
        }
    }

    return 0;
}

int DBImpl::PostPaxosMsg(const paxos::Message& msg)
{
    std::unique_ptr<paxos::Message> rsp_msg = nullptr;
    {
        dbcomm::HashLock hashlock(
                hashbase_lock_.get(), 
                option_.pfn_hash32(msg.key()));
        hashlock.WriteLock();

        auto ret = PostPaxosMsgNoLock(msg, rsp_msg);
        if (0 != ret) {
            logerr("PostPaxosMsgNoLock ret %d", ret);
            return ret;
        }

        assert(0 == ret);
    }

    if (nullptr != rsp_msg) {
        msg_sender_->SendMsg(std::move(rsp_msg));
    }

    return 0;
}

int DBImpl::SetNoLockNoWrite(
        const std::string& key, 
        uint64_t reqid, 
        const std::string& value, 
        const uint32_t* prev_version, 
        uint32_t& new_version, 
        paxos::PaxosLogHeader& header, 
        paxos::PaxosLog& plog, 
        std::unique_ptr<paxos::PLogWrapper>& wrapper, 
        std::shared_ptr<paxos::PInsAliveState>& alive_state, 
        std::unique_ptr<paxos::Message>& rsp_msg)
{
    auto ret = option_.pfn_read(key, header, plog);
    if (0 != ret) {
        logerr("pfn_read_ ret %d", ret);
        return ret;
    }
    
    assert(0 == ret);
    AssertCheck(header, plog);
    ret = paxos::can_write(plog);
    if (0 != ret) {
        logerr("can_write ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    assert(0 == header.max_index() || 0 < header.chosen_index() || 1 == header.max_index());
    if (nullptr != prev_version) {
        if (*prev_version != header.version()) {
            logerr("prev_version %u header.version %lu", 
                    *prev_version, header.version());
            return PAXOS_SET_VERSION_OUT;
        }
    }

    std::string raw_value;
    ret = packRawValue(header, value, raw_value);
    if (0 != ret) {
        return ret;
    }

    assert(0 == ret);
    assert(false == raw_value.empty());

    uint8_t selfid = option_.pfn_selfid(key);
    wrapper = cutils::make_unique<paxos::PLogWrapper>(
            selfid, member_id_, key, nullptr, plog);
    assert(nullptr != wrapper);

    assert(nullptr == alive_state);
    std::tie(ret, alive_state, rsp_msg) = 
        wrapper->Set(reqid, raw_value, true);
    if (0 != ret) {
        assert(nullptr == rsp_msg);
        assert(nullptr == alive_state);
        logerr("PLogWrapper::Set ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    assert(nullptr != rsp_msg);
    assert(rsp_msg->index() == alive_state->GetIndex());
    assert(paxos::get_max_index(plog) == rsp_msg->index());
    assert(wrapper->NeedDiskWrite());

    // small update
    // header.set_max_index(paxos::get_max_index(plog));
    assert(0 == updateHeader(plog, header));
    AssertCheck(header, plog);
    return 0;
}

int DBImpl::DoWriteNoLock(
        const paxos::PLogWrapper& wrapper, 
        const paxos::PaxosLogHeader& header, 
        const paxos::PaxosLog& plog)
{
    AssertCheck(header, plog);
    if (false == wrapper.NeedDiskWrite()) {
        return 0; // do nothing;
    }

    assert(wrapper.NeedDiskWrite());
    return option_.pfn_write(wrapper.GetKey(), header, plog);
}

int DBImpl::SetNoLock(
        const std::string& key, 
        uint64_t reqid, 
        const std::string& value, 
        const uint32_t* prev_version, 
        uint32_t& new_version, 
        std::shared_ptr<paxos::PInsAliveState>& alive_state)
{
    paxos::PaxosLogHeader header;
    paxos::PaxosLog plog;

    std::unique_ptr<paxos::PLogWrapper> wrapper = nullptr;
    std::unique_ptr<paxos::Message> rsp_msg = nullptr;

    auto ret = SetNoLockNoWrite(
            key, reqid, value, prev_version,  new_version, 
            header, plog, wrapper, alive_state, rsp_msg);
    if (0 != ret) {
        logerr("SetNoLockNoWrite ret %d", ret);
        assert(nullptr == wrapper);
        assert(nullptr == alive_state);
        assert(nullptr == rsp_msg);
        return ret;
    }

    assert(0 == ret);
    assert(nullptr != wrapper);
    assert(nullptr != alive_state);
    assert(nullptr != rsp_msg);
    assert(alive_state->GetIndex() == rsp_msg->index());
    assert(wrapper->NeedDiskWrite());

    ret = DoWriteNoLock(*wrapper, header, plog);
    if (0 != ret) {
        alive_state_cache_->Destroy(key, alive_state->GetIndex());
        alive_state = nullptr;
        rsp_msg = nullptr;
        logerr("DoWriteNoLock ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    // insert into
    ret = alive_state_cache_->Insert(alive_state, true);
    assert(0 == ret);
    alive_state_cache_->AddTimeout(*alive_state);
    msg_sender_->SendMsg(std::move(rsp_msg)); 
    return 0;
}

int DBImpl::CheckReqID(
        const std::string& key, 
        uint64_t set_on_index, 
        uint64_t reqid)
{
    paxos::PaxosLogHeader header;
    auto ret = option_.pfn_read_header(key, header);
    if (0 != ret) {
        logerr("pfn_read_header_ ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    if (set_on_index != header.chosen_index() || 
            reqid != header.reqid()) {
        return PAXOS_SET_PREEMPTED;
    }

    assert(set_on_index == header.chosen_index());
    assert(reqid == header.reqid());
    return 0;
}

int DBImpl::Set(
        const std::string& key, 
        const std::string& value, 
        const uint32_t* prev_version, 
        uint64_t& forward_reqid, 
        uint32_t& new_version)
{
    if (key.empty()) {
        return -1;
    }

    assert(false == key.empty());

    uint64_t reqid = (*idgen_)();
    
    std::shared_ptr<paxos::PInsAliveState> alive_state = nullptr;
    {
        {
            dbcomm::HashLock hashlock(
                    hashbase_lock_.get(), 
                    option_.pfn_hash32(key));
            hashlock.WriteLock();

            auto ret = SetNoLock(
                    key, reqid, value, 
                    prev_version, new_version, alive_state);
            if (0 != ret) {
                logerr("SetNoLock ret %d", ret);
                assert(nullptr == alive_state);
                return ret;
            }

            assert(0 == ret);
            assert(nullptr != alive_state);
            assert(0 < alive_state->GetIndex());
        }
    }

    assert(nullptr != alive_state);
    auto ret = WaitForNotify(
            alive_state->GetNotifyFD(), option_.max_set_timeout);
    if (0 != ret) {
        logerr("WaitForNotify ret %d", ret);
        return PAXOS_SET_TIME_OUT;
    }

    assert(0 == ret);
    alive_state_cache_->Destroy(key, alive_state->GetIndex());
    ret = CheckReqID(key, alive_state->GetIndex(), reqid);
    if (0 != ret) {
        logerr("CheckReqID index %lu reqid %lu ret %d", 
                alive_state->GetIndex(), reqid, ret);
        return ret;
    }

    assert(0 == ret);
    AssertCheckValue(key, alive_state->GetIndex(), reqid, value);
    return 0;
}


int DBImpl::Get(
        const std::string& key, 
        uint64_t other_max_index, 
        uint8_t peer_status, 
        std::string& value, 
        uint32_t& version)
{
    paxos::PaxosLogHeader header; 
    paxos::PaxosLog plog;
    auto ret = option_.pfn_read(key, header, plog);
    if (0 != ret) {
        logerr("pfn_read ret %d", ret);
        return ret;
    }

    assert(0 == ret);
    AssertCheck(header, plog);
    ret = paxos::can_read_3svr(
            header.chosen_index(), 
            header.max_index(), other_max_index, peer_status);
    if (0 != ret) {
        if (PAXOS_GET_MAY_LOCAL_OUT != ret) {
            MayTriggerACatchUp(ret, key, header.max_index());
            return ret;
        }

        assert(PAXOS_GET_MAY_LOCAL_OUT == ret);
        assert(other_max_index == header.max_index());
        assert(other_max_index == header.chosen_index() + 1);

        auto alive_state= alive_state_cache_->Get(
                key, header.max_index());
        if (nullptr == alive_state) {
            // retry redo
            int err = TryRedo(key, header.max_index(), true);
            if (0 > err) {
                logerr("TryRedo err %d", err);
                return ret;
            }

            assert(0 == err || 1 == err);
            if (0 == err) {
                alive_state = 
                    alive_state_cache_->Get(key, header.max_index());
            }
        }

        if (nullptr != alive_state) {
            if (option_.max_concur_wait < alive_state.use_count()) {
                return PAXOS_GET_MAX_CONCUR_WAIT_COUNT;
            }

            int notify_fd = alive_state->GetNotifyFD();
            assert(0 <= notify_fd);
            int err = WaitForNotify(
                    notify_fd, option_.max_get_waittime);
            if (0 != err) {
                return ret;
            }

            assert(0 == err);
        }

        // update
        uint64_t prev_chosen_index = header.chosen_index();
        ret = option_.pfn_read(key, header, plog);
        if (0 != ret) {
            logerr("pfn_read ret %d", ret);
            return ret;
        }

        assert(0 == ret);
        if (prev_chosen_index == header.chosen_index()) {
            return PAXOS_GET_MAY_LOCAL_OUT;
        }

        assert(prev_chosen_index < header.chosen_index());
        assert(other_max_index <= header.chosen_index());
    }

    assert(0 == ret);
    value.clear();
    version = header.version();
    auto chosen_ins = paxos::get_chosen_ins(plog);
    if (nullptr == chosen_ins || 0 == chosen_ins->index()) {
        // empty
        return 0;
    }

    assert(nullptr != chosen_ins);
    assert(chosen_ins->has_accepted_value());
    paxos::DBData data;
    if (false == data.ParseFromString(
                chosen_ins->accepted_value().data())) {
        return BROKEN_DBDATA;
    }

    assert(data.version() == header.version());
    value = data.value();
    return 0;
}


int DBImpl::BatchTriggerCatchUpOn(
        const std::vector<std::string>& keys, 
        const std::vector<uint8_t>& peers)
{
    // TODO
    return -1;
}

int DBImpl::TryRedo(
        const std::string& key, 
        uint64_t max_index, 
        bool is_read_redo) 
{
    // TODO
    return -1;
}

void DBImpl::MayTriggerACatchUp(
        int ret_code, 
        const std::string& key, 
        uint64_t max_index)
{
    switch (ret_code) {
    case PAXOS_GET_LOCAL_OUT:
    case PAXOS_GET_MAY_LOCAL_OUT:
    case PAXOS_GET_WEIRD:
        localout_helper_->Add(key, max_index);
        break;
    default:
        break;
    }

    return ;
}

void DBImpl::TriggerACatchUpOn(const std::string& key)
{
    uint8_t selfid = option_.pfn_selfid(key);
    auto catch_up_msg = BuildACatchUpMsg(key, selfid);
    assert(nullptr != catch_up_msg);
    msg_sender_->SendMsg(std::move(catch_up_msg));
    return ;
}

int DBImpl::StartGetLocalOutWorker()
{
    return localout_helper_->StartWorker(*this);
}

int DBImpl::StartTimeoutWorker()
{
    return alive_state_cache_->StartTimeoutThread(*this);
}

} // namespace paxoskv


