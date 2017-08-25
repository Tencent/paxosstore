
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once


enum {
	SERIALIZE_PAXOS_LOG_ERR = -50000, 
	SERIALIZE_DBDATA_ERR = -50001,
	SERIALIZE_PAXOS_MESSAGE_ERR = -50002, 
	SERIALIZE_FLAG_LOG_ERR = -50003, 
	SERIALIZE_PAXOS_KEY_ERR = -50004, 

	BROKEN_PAXOS_LOG = -50100, 
	BROKEN_DBDATA = -50101, 
	BROKEN_DB_RECORD = -50102, 
	BROKEN_DB_COMPRESSE_RECORD = -50103, 
	BROKEN_PAXOS_MESSAGE = -50104, 
	BROKEN_FLAG_LOG = -50105, 
	BROKEN_PAXOS_KEY = -50106, 
	BROKEN_BITCASK_RECORD = -50107, 
	BROKEN_LEVELDB_RECORD = -50108, 

	BROKEN_DB_HEADER = -50198, 
	BROKEN_UNCOMPRESSE_ERR = -50199, 

	PAXOS_PENDING = -50200, 
	PAXOS_SET_VERSION_OUT = -50201, 
	PAXOS_SET_TIME_OUT = -50202, 
	PAXOS_SET_PREEMPTED = -50203, 
	PAXOS_SET_LOCAL_OUT = -50204,
	PAXOS_SET_MAY_LOCAL_OUT = -50205, 

	PAXOS_SET_PLOG_LEARNER_ONLY = -50221, 
	PAXOS_SET_FAST_RETURN_TEST = -50222, 
	PAXOS_SET_REACH_MAX_VALUE = -50223, 

	PAXOS_GET_LOCAL_OUT = -50300,
	PAXOS_GET_MAY_LOCAL_OUT = -50301, 
	PAXOS_GET_WEIRD = -50302, 
	PAXOS_INVALID_PEER_STATUS = -50303, 
	PAXOS_GET_MAX_CONCUR_WAIT_COUNT = -50304, 

	PAXOS_UDP_RECV_ALL_ERROR = -50400, 
	PAXOS_UDP_MAX_RETRY = -50401, 
	PAXOS_INVALID_MSG = -50402, 
	PAXOS_RESOURCE_LIMIT = -50403, 
	PAXOS_STOP_BY_UNSAFE_PLOG = -50404, 
	PAXOS_DISK_WRITE_ERR = -50405, 
	PAXOS_LEVELDB_READ_ERR = -50410, 
	PAXOS_LEVELDB_WRITE_ERR = -50411, 
	PAXOS_BITCASKDB_KEY_MISMATCH = -50412, 

	PAXOS_INNER_META_KEY = -50499, 
	PAXOS_BATCH_ADD_ERR = -50500, 
	PAXOS_REMOVE_HASH_KEY_MISS_MATCH = -50501, 

	FAKE_LOCAL_TRANS_BATCH_SET_ERROR = -61001, 
};


