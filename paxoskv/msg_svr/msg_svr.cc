
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <unistd.h>
#include <netinet/in.h>
#include <tuple>
#include <sstream>
#include "msg_svr.h"
#include "msg_cli.h"
#include "core/paxos.pb.h"
#include "core/err_code.h"
#include "cutils/mem_utils.h"
#include "cutils/log_utils.h"
#include "cutils/cqueue.h"
#include "msg_comm.h"

#include "msg_epoll.h"
#if defined(__MSGSVR_USE_LIBCO__)
#include "co_epoll.h"
#endif

namespace {


class PLogClientSession {

public:
	PLogClientSession(
			const int efd, struct sockaddr_in cli_addr)
        : efd_(efd)
        , client_ip_(cli_addr.sin_addr.s_addr)
		, close_mark_(false)
		, cli_fd_(-1)
		, recv_size_(0)
	{
		assert(0 <= efd_);
		memcpy(&cli_addr_, &cli_addr, sizeof(sockaddr_in));
		memset(&read_ev_, 0, sizeof(epoll_event));
		recv_buffer_.resize(32 * 1024 * 1024);
	}

	~PLogClientSession()
	{
		if (-1 != cli_fd_) {
			assert(0 <= cli_fd_);
#if defined(__MSGSVR_USE_LIBCO__)
            co_epoll_ctl(efd_, EPOLL_CTL_DEL, cli_fd_, nullptr);
#else
			epoll_ctl(efd_, EPOLL_CTL_DEL, cli_fd_, nullptr);
#endif
			close(cli_fd_);
			cli_fd_ = -1;
		}
	}

	int Init(int cli_fd)
	{
		assert(0 <= efd_);
		assert(-1 == cli_fd_);
		assert(0 == recv_size_);

		read_ev_.data.ptr = this;
		assert(NULL != read_ev_.data.ptr);
		read_ev_.events = EPOLLIN | EPOLLHUP | EPOLLERR;

#if defined(__MSGSVR_USE_LIBCO__)
        int ret = co_epoll_ctl(efd_, EPOLL_CTL_ADD, cli_fd, &read_ev_);
#else
		int ret = epoll_ctl(efd_, EPOLL_CTL_ADD, cli_fd, &read_ev_);
#endif
		if (0 != ret) {
			memset(&read_ev_, 0, sizeof(read_ev_));
			return ret;
		}

		cli_fd_ = cli_fd;
		return 0;
	}

	int HandleRead()
	{
		assert(0 <= cli_fd_);
		if (recv_size_ == recv_buffer_.size()) {
			return 1; // full;
		}

		assert(0 < recv_buffer_.size() - recv_size_);
		errno = 0;
		int read_len = read(
				cli_fd_, 
				&(recv_buffer_[recv_size_]), 
                recv_buffer_.size() - recv_size_);
		if (0 >= read_len) {
			if (0 == read_len) {
				close_mark_ = true;
			}

			if (-1 == read_len && EAGAIN == errno) {
				return 0;
			}

			return read_len;
		}

		assert(0 < read_len);
		assert(recv_size_ + read_len <= recv_buffer_.size());
		recv_size_ += read_len;
		return recv_size_ == recv_buffer_.size() ? 1 : 0;
	}

	int HandleRecvBuffer(
			size_t& handle_msg_cnt, 
			std::map<int, 
                std::vector<std::unique_ptr<
                    paxos::Message>>>& map_msg_buffer, 
			std::map<paxos::MessageType, uint64_t>& map_msg_stat)
	{
		assert(0 == handle_msg_cnt);
		if (0 == recv_size_) {
			return 0;
		}

        auto& vec_msg = map_msg_buffer[client_ip_];
		size_t handled_size = 0;
		while (handled_size < recv_size_) {
			size_t msg_size = 0;
			auto msg = cutils::make_unique<paxos::Message>();
			assert(nullptr != msg);
			int ret = pickle_msg(
					&recv_buffer_[handled_size], 
					recv_size_ - handled_size, 
					msg_size, 
					*msg);
			if (0 > ret) {
				logerr("pickle_msg client_ip %d "
                        "cli_fd_ %d handled_size %zu "
						"recv_size_ %zu ret %d", 
						client_ip_, 
						cli_fd_, handled_size, recv_size_, ret);
				return ret;
			}

			assert(0 <= ret);
			if (1 == ret) {
				break;
			}

			assert(0 == ret);
			assert(0 < msg_size);
			assert(handled_size + msg_size <= recv_size_);
			assert(NULL != msg);

			++map_msg_stat[msg->type()];
            vec_msg.push_back(std::move(msg));
			handled_size += msg_size;
			++handle_msg_cnt;
		}

		assert(handled_size <= recv_size_);
		if (0 != handled_size) {
			if (0 != (recv_size_ - handled_size)) {
				memmove(&recv_buffer_[0], 
						&recv_buffer_[handled_size], 
						recv_size_ - handled_size);
			}
			recv_size_ -= handled_size;
		}

		return 0;
	}

	bool GetCloseMark() const {
		return close_mark_;
	}

	int GetClientFD() const {
		return cli_fd_;
	}

	struct epoll_event& GetEpollEvent() {
		return read_ev_;
	}

	const std::string GetClientIP() const {
		return paxos::to_string_ip(cli_addr_.sin_addr.s_addr);	
	}

private:
	const int efd_ = -1;
    const int client_ip_ = 0;
	struct sockaddr_in cli_addr_;
	bool close_mark_ = false;
	int cli_fd_ = -1;
	struct epoll_event read_ev_;

	size_t recv_size_ = 0;
	std::vector<char> recv_buffer_;
}; // class PLogClientSession


size_t process_read(
		struct epoll_event& ev, 
		std::map<int, std::vector<
            std::unique_ptr<paxos::Message>>>& map_msg_buffer, 
		std::map<paxos::MessageType, uint64_t>& map_msg_stat)
{
    auto session = reinterpret_cast<PLogClientSession*>(ev.data.ptr);
	assert(nullptr != session);
	assert(0 <= session->GetClientFD());

	int ret = session->HandleRead();
	if (0 > ret) {
		std::string client_ip = session->GetClientIP();
		logerr("client %s HandleRead ret %d", client_ip.c_str(), ret);
		delete session;
		return 0;
	}

	assert(0 <= ret);
	size_t handle_msg_cnt = 0;
	ret = session->HandleRecvBuffer(
            handle_msg_cnt, map_msg_buffer, map_msg_stat);
	if (0 != ret) {
		std::string client_ip = session->GetClientIP();
		logerr("client %s HandleRecvBuffer ret %d", client_ip.c_str(), ret);
		delete session;
		return handle_msg_cnt;
	}

	if (session->GetCloseMark()) {
		std::string client_ip = session->GetClientIP();
		logerr("client %s cli_fd %d Close", 
				client_ip.c_str(), session->GetClientFD());
		delete session;
	}

	return handle_msg_cnt;
}


int proess_accept(
		const int efd, 
		struct epoll_event& ev, 
		size_t& acc_msg_cnt, 
		paxos::SmartPaxosMsgRecver& msg_reciver, 
		std::map<int, std::vector<
            std::unique_ptr<paxos::Message>>>& map_msg_buffer, 
		std::map<paxos::MessageType, uint64_t>& map_msg_stat)
{
	assert(nullptr != ev.data.ptr);
	int listen_fd = *reinterpret_cast<int*>(ev.data.ptr);
	assert(0 <= listen_fd);

	struct sockaddr_in cli_addr = {0};
	int accpt_cnt = 0;
	for (size_t itry = 0; itry < 1000; ++itry) {
		int cli_len = sizeof(struct sockaddr_in);
		errno = 0;
		int cli_fd = accept(
				listen_fd, 
				reinterpret_cast<struct sockaddr *>(&cli_addr), 
				reinterpret_cast<socklen_t*>(&cli_len));
		if (0 > cli_fd) {
			if (EAGAIN != errno) {
				logerr("accept errno %d (%s)", errno, strerror(errno));
			}

			break;
		}

		++accpt_cnt;
		assert(0 <= cli_fd);
        int ret = paxos::set_non_block(cli_fd);
		if (0 != ret) {
			logerr("set_non_block cli_fd %d ret %d", cli_fd, ret);
			close(cli_fd);
			break;
		}

		assert(0 == ret);

		// TODO: 
        auto session = new PLogClientSession(efd, cli_addr);
		assert(nullptr != session);
		ret = session->Init(cli_fd);
		if (0 != ret) {
			logerr("PLogClientSession::Init cli_fd %d ret %d", 
					cli_fd, ret);
			delete session;
			close(cli_fd);
			break;
		}

		assert(0 == ret);
		struct epoll_event fake_ev = {0};
		fake_ev.data.ptr = session;
		fake_ev.events = EPOLLIN;
		acc_msg_cnt += process_read(
                fake_ev, map_msg_buffer, map_msg_stat);
	}

	return accpt_cnt;
}

void msg_svr_loop(
		int listen_fd, 
		size_t svr_idx, 
		paxos::SmartPaxosMsgRecver& msg_reciver, 
		bool& stop)
{
	assert(0 <= listen_fd);

	printf ( "START: listen_fd %d svr_idx %zu\n", listen_fd, svr_idx );
    // bind cpu
    paxos::bind_to_cpu(svr_idx + 1, svr_idx + 2);

	const int MAX_EPOLL_EVENT_CNT = 1000;

#if defined(__MSGSVR_USE_LIBCO__)
    int efd = co_epoll_create(MAX_EPOLL_EVENT_CNT);
#else
	int efd = epoll_create(MAX_EPOLL_EVENT_CNT);
	assert(0 <= efd);
#endif
	
	int ret = 0;
	struct epoll_event listen_ev = {0};
	{
		listen_ev.data.ptr = &listen_fd;
		listen_ev.events = EPOLLIN | EPOLLHUP | EPOLLERR;
#if defined(__MSGSVR_USE_LIBCO__)
        ret = co_epoll_ctl(
                efd, EPOLL_CTL_ADD, listen_fd, &listen_ev);
#else
		ret = epoll_ctl(efd, EPOLL_CTL_ADD, listen_fd, &listen_ev);
#endif
		assert(0 == ret);
	}

#if defined(__MSGSVR_USE_LIBCO__)
    co_epoll_res* arr_active_event = 
        co_epoll_res_alloc(MAX_EPOLL_EVENT_CNT);
    assert(nullptr != arr_active_event);
#else
	struct epoll_event arr_active_event[MAX_EPOLL_EVENT_CNT] = {0};
#endif
	
	size_t acc_msg_cnt = 0;
	std::map<int, std::vector<
        std::unique_ptr<paxos::Message>>> map_msg_buffer;
	std::map<paxos::MessageType, uint64_t> map_msg_stat;

    size_t loop_cnt = 0;
	while (false == stop) {
        ++loop_cnt;

#if defined(__MSGSVR_USE_LIBCO__)
        int active_cnt = co_epoll_wait(
                efd, arr_active_event, MAX_EPOLL_EVENT_CNT, 1);
#else
		int active_cnt = epoll_wait(
                efd, arr_active_event, MAX_EPOLL_EVENT_CNT, 1);
#endif

		for (int idx = 0; idx < active_cnt; ++idx) {
#if defined(__MSGSVR_USE_LIBCO__)
            struct epoll_event& 
                cur_ev = arr_active_event->events[idx];
#else
			struct epoll_event& cur_ev = arr_active_event[idx];
#endif
			if (&listen_fd == cur_ev.data.ptr) {
				int accpt_cnt = 0;
				accpt_cnt = proess_accept(
						efd, cur_ev, 
						acc_msg_cnt, msg_reciver, 
                        map_msg_buffer, map_msg_stat);
				logdebug("proess_accept accpt_cnt %d", accpt_cnt);
				continue;
			}

			assert(&listen_fd != cur_ev.data.ptr);
			assert(NULL != cur_ev.data.ptr);
			acc_msg_cnt += process_read(
                    cur_ev, map_msg_buffer, map_msg_stat);
		}

		if (0 != acc_msg_cnt) {
			acc_msg_cnt = enqueue_msg(msg_reciver, map_msg_buffer);
		}
		
		assert(0 == acc_msg_cnt);
		if (0 == loop_cnt % 1000) {
			std::stringstream ss;
			for (auto iter = map_msg_stat.begin(); 
                    iter != map_msg_stat.end(); ++iter) {
				ss << " [" << iter->first << ", " << iter->second << "]";
			}

			std::string stat_msg = ss.str();
			logerr("STAT (%zu) %s", 
                    map_msg_stat.size(), stat_msg.c_str());
		}
	}

#if defined(__MSGSVR_USE_LIBCO__)
    co_epoll_ctl(efd, EPOLL_CTL_DEL, listen_fd, &listen_ev);
#else
	epoll_ctl(efd, EPOLL_CTL_DEL, listen_fd, &listen_ev);
#endif
	close(efd);

	return ;
}



}


namespace paxos {


void start_multiple_msg_svr(
		const char* svr_ip, 
		int svr_port, 
		size_t num_of_svr, 
		SmartPaxosMsgRecver& msg_reciver, 
		bool& stop)
{
	assert(nullptr != svr_ip);

	assert(0 < num_of_svr);
	assert(24 > num_of_svr);

    int listen_fd = listen_at(svr_ip, svr_port, 1024);
	assert(0 <= listen_fd);

    auto ret = set_non_block(listen_fd);
	assert(0 == ret);

	{
		std::vector<std::future<void>> workers;
		for (size_t idx = 0; idx < num_of_svr; ++idx) {

			workers.emplace_back(
					std::async(
						std::launch::async, 
						msg_svr_loop, 
						listen_fd, idx, 
                        std::ref(msg_reciver), std::ref(stop)));
			assert(workers.back().valid());
		}

		for (auto& worker : workers) {
            worker.get();
		}
	}

	printf ( "END: try close listen_fd %d\n", listen_fd );
	close(listen_fd);
	return ;
}




} // namespace paxos;



