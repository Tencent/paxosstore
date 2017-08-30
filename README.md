PaxosStore
===========

PaxosStore is a distributed-database inspired by Google MegaStore, it's 2nd generation of storage system used widely in WeChat. 
PaxosStore has been deployed in WeChat production for more than two years, providing storage services for the core businesses of WeChat backend including user account management,user relationship management (i.e., contacts), instant messaging, social networking, and online payment. 

Now PaxosStore is running on thousands of machines, with nealy billions of peak TPS. 

Before PaxosStore, We have QuorumKV storage system, which have powered WeChat service with strong consistent read/write since 2011. As the number of storage server hit tens of thousand, the operation mantenance and development of a NWR-based service in such large scale is becoming a painful work. That's why we come up with PaxosStore: a new generation of distributed-database system, on top of leaseless paxos consensus layers, provied
 - __Two paxos consensue libraries__(open source now): 
   - **certain** component for normal PaxosLog + DB design;
   - **paxoskv** component for Key-Value(PaxosLog-as-value);

 (The following items are planned for open source from September to October)
 - __A high performance Key-Value system__
 - __A system that supports rich data structures such as queues and collections__ 
 - __A high performance storage engine based on LSM-tree__ 
 - __A New SQL-like Table system__
 

### PaxosStore Architecture

![image](images/overall_architecture.jpg)

For for technical details of PaxosStore, please refer to following paper/articles:
- [VLDB 2017 PaxosStore: High-availability Storage Made Practical in WeChat](http://www.vldb.org/pvldb/vol10/p1730-lin.pdf)
- [微信PaxosStore：深入浅出Paxos算法协议](http://www.infoq.com/cn/articles/wechat-paxosstore-paxos-algorithm-protocol)  
- [微信PaxosStore内存篇：十亿Paxos/分钟的挑战](http://www.infoq.com/cn/articles/one-billion-paxos-minutes-of-challenge)
- [微信后台基于时间序的海量数据冷热分级架构设计实践](https://mp.weixin.qq.com/s/XlZF0GDt7dnHyYuS1an6tg)


### Build

- [Certain](./certain)
- [PaxosKV](./paxoskv)

### License

PaxosStore is under the BSD 3-Clause License. See the [LICENSE.txt](./LICENSE.txt) file for details.
