## paxos kv

一直觉得etcd/raft的写法很简洁, 写的paxos kv组件也很大程度上受到前者的启发. 本文将相对细致的介绍paxos kv组件, 更具体的说, 包含以下组成部分: 
 - paxos state machine: paxos算法
 - paxos message server: 负责paxos message的收发\处理
 - paxos db: 定义读写接口, paxos消息处理接口

### paxos state machine 
``` c++
    std::tie(new_plog, msg_rsp) = step(plog, msg)
```
paxos状态机可以简述为上述等式: 状态机, 即step函数, 根据paxos协议, 处理接收到的paxos message, 最终输出是paxos log本地状态的修改(从plog到new_plog), 以及待发送的消息响应. 比如说: 消息代表paxos Prepare阶段请求, 状态机器发现本地可以Promised该请求时, 将修改本地paxos log对应状态, 并准备响应告知对端Promised.

#### paxos log
``` protobuf
message PaxosInstance {
    required uint64 index = 1;
    optional uint64 proposed_num = 2; 
    optional uint64 promised_num = 3;
    optional uint64 accepted_num = 4;
    optional Entry accepted_value = 5;
    required bool chosen_flag = 6;
}

message PaxosLog {
    repeated PaxosInstance entries;
}
```
paxos log的pb接口如上所示, 这里需要详细说明的是PaxosLog是由多个PaxosInstance(即log entry)组成, 更具体的说对应到plog as db:
 - entries_size == 0: plog为空
 - entries_size == 1: 对应plog处于pending/chosen状态
 - entries_size == 2:
    - chosen_index i, pending_index i + 1;
    - pending_index i, pending_index i + 1; 其中 pending_index i + 1不存在accpted_value
所有entires_size > 2或者entries_size == 2但不属于上述列出的情况下的plog, 均可以精简到以上某种情况中. 特别是: 
 - 当存在2个PaxosInstance, 且对应index不连续时, 只有最新PaxosInstance有意义; 
 - 当存在2个PaxosInstance, index连续且均存在accepted_value时, 只有最新的PaxosInstance有意义; 

#### paxos message
``` protobuf
message Message {
    required MessageType type = 1;
    required uint32 from = 2;
    required uint32 to = 3;
    required uint64 key = 4; 
    required uint64 index = 5;
    required uint64 proposed_num = 6;
    optional uint64 promised_num = 7;
    optional uint64 accept ed_num = 8;
    optional Entry accepted_value = 9;
}
```
消息的类型大概有以下几种: 
 - PROP, PROP_RSP: Prepare流程的请求和响应
 - ACCPT, ACCPT_RSP: Accept流程的请求和响应
 - FAST_ACCPT, FAST_ACCPT_RSP: 即预授权的Accept流程的请求和响应; 
 - CHOSEN, GET_CHOSEN: 用于交互Chosen消息

#### 状态机函数 step: 
 1. 根据消息index, 对应到paxos log中的log entry;
 2. 根据log entry是否chosen: 
     - stepChosen: 
        本地已chosen, 对Prepare/Accept/GetChosen等消息返回Chosen响应
     - stepNotChosen:
        本地未chosen, 则根据请求类型:
        - Chosen请求, 接受消息中的accpted_value, 并将log entry标记为chosen;
        - Prepare/Accept请求, 则根据协议修改log entry对应状态, 返回PreparePrsp/AcceptRsp;
        - PrepareRsp/AcceptRsp请求, 首先验证消息的有效性, 即msg.proposed_num与log.proposed_num是否对应; 而后根据消息中携带的Promised/Accepted推进paxos过程: 收到多数派Promised后状态机从Prepare阶段推进成Accept, 返回广播Accept; 收到多数派Accepted后状态机从Accept推进成Chosen, 返回广播Chosen. 
           备注: 实际实现中, 引入AliveState跟踪Prepare/Accept阶段的投票情况. 
 3. 返回new_plog, msg_rsp
     这里需要澄清的是, step函数并不一定输出new_plog, msg_rsp. 比如: 当收到过期的PrepareRsp时, 本地plog不发生改变, 也不需要作出响应; 当收到GetChosen消息时, 本地plog不发生变化, 当且仅当本地有Chosen log entry时返回Chosen响应.
 
### paxos message server
大致组成如下: 
 - 消息发送队列和worker
    worker从队列中取出消息, 并根据<msg.key, msg.to>发送给对端paxos message server.  
 - 消息接收队列和worker
    worker从队列中取出消息, 调用本地paxos db的消息处理接口.
 - epoll server
    监听13069端口, 将收到的消息放入消息接收队列. 

### paxos db
读写接口是面向上层分布式存储server; 消息处理接口则面向paxos message server. 

#### paxos消息处理接口
``` c++ 
std::lock_guard<std::mutex> lock(mutex_);
plog = read(msg.key());
std::tie(new_plog, rsp_msg) = step(plog, message);
write(msg.key(), new_plog);
send(rsp_msg);
```
消息处理接口的逻辑基本上如上段伪代码所示(忽略错误处理), 实际实现中需额外考虑一些情况: 
  1. step函数并不一定产生需要写回的new_plog, 和需要发送的rsp_msg; 
  2. 当本地有等待在index i上的写请求时候(具体见写接口), 发生以下情况时需要唤醒对应写等待: 
      - 对应log entry进入Chosen状态; 
      - 收到msg.index > index i 时, 表示本地数据落后了;

#### paxos读接口
简单的说, 读接口即读当前的plog, 并判定是否是最新数据(分布式角度). 更具体的说: 
 - 读接口只返回chosen的数据;
 - 读接口根据其他机器(A/B/C)对应plog的位置判定本地数据是否最新:
     LocalChosenIndex代表本地chosen对应的log序号; OtherMaxIndex代表收集到其他机器最大的log序号; 
    - LocalChosenIndex >= OtherMaxIndex	        本地chosen数据最新
    - LocalChosenIndex + 1 == OtherMaxIndex 	当且仅当OtherMaxIndex均为Pending时, 本地chosen数据最新
    - LocalChosenIndex + 1 < OtherMaxIndex	        本地数据落后

    当可以确定本地chosen数据最新时, 直接返回本地chosen数据; 否则, 视情况发起等待: 
    - LocalChosenIndex + 1 == OtherMaxIndex 且无法确定OtherMaxIndex均为Pending时, 等待本地写完成或者主动发起重做写并等待. 
    - LocalChosenIndex + 1 < OtherMaxIndex, 目前的处理逻辑是触发异步CatchUp, 并返回失败. 
    备注: 
    1. 当LocalChosenIndex + 1 == OtherMaxIndex 且OtherMaxIndex均为Pending时, 可以选择等待写完成或者重做写并等待, 但为了性能选择认为本地数据最新(也确实是如此); 
    2. 当LocalChosenIndex + 1 < OtherMaxIndex时, 可以在触发异步CatchUp的基础上, 等待异步CatchUp, 但目前没做; 

#### paxos写接口
``` c++
std::lock_guard<std::mutex>lock(mutex_);
plog = read(key);
std::tie(new_plog, msg) = set(plog, value);
write(key, new_plog);
send(msg);
```
简化版本的写逻辑如上述伪码所示, 其中set函数逻辑如下: 
 - plog.has_pending
    - chosen_index + 1 != max_index 写本地落后, 触发异步catchup并失败; 
    - chosen_index + 1 == max_index 本地尝试抢占写, 即试图用value作为候选的提议值重新发起Prepare流程; 
 - false == plog.has_pending
    当chosen数据是本svr提议, 则认为可以尝试直接走accept流程; 否则走Prepare流程.  


