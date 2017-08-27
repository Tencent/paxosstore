
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <netinet/in.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <memory>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <cassert>
#include "cutils/log_utils.h"
#include "conhash.h"
#include "svrlist_config_base.h"
#include "iconfig.h"
#include "kvsvrcomm.h"

#define _err_line -1000 - __LINE__

using namespace Comm;

template <class T>
class clsDelayFree
{
	T * m_ptr;
public:
	clsDelayFree()
	{
		m_ptr = NULL;
	}
	void Free( T * p )
	{
		if( m_ptr )
		{
			delete m_ptr;
			m_ptr = 0;
		}
		m_ptr = p;
	}
	T *Take()
	{
		T * p = m_ptr;
		m_ptr = NULL;
		return p;
	}
	T * Get()
	{
		return m_ptr;
	}

	~clsDelayFree()
	{
		if( m_ptr ) delete m_ptr;
		m_ptr = NULL;
	}
};
class param 
{
public:
	/*
	[Server]
	ServerCount=
	KvConsistentHash=
	*/
	uint32_t _iServerCount;// = 0;
	uint32_t _iConsistentHash;// = 0;

	/*
	[General]
	GroupCount=
	FailPeriod=
	FailCount=
	TimeOut=
	*/

	uint32_t _iGroupCount;// = 6;
	uint32_t _iFailPeriod;// = 60;
	uint32_t _iFailCount;// = 60;
	uint32_t _iTimeOut;// = 500;

	clsConHash *_poConHash;// = NULL;
    std::vector< SvrGroup_t * > *pvGroups;

	SvrGroupList_t *pGroupList4SelfIP;

	param()
	{
		memset( this,0,sizeof(*this) ); //WARNING !!!
	}
	~param()
	{
		if( _poConHash ) delete _poConHash;_poConHash = NULL;
		if( pvGroups ) delete pvGroups;pvGroups = NULL;
		if( pGroupList4SelfIP ) delete pGroupList4SelfIP;pGroupList4SelfIP = NULL;
	}
	void SetConnHash( clsConHash *p )
	{
		FreeConHash();
		_poConHash = p;
	}
	void SetGroups( std::vector< SvrGroup_t *> *p )
	{
		FreeGroups();
		pvGroups = p;
	}
	void SetGroupList4SelfIP( SvrGroupList_t *p )
	{
		FreeGroupList4SelfIP();
		pGroupList4SelfIP = p;
		
	}
	void FreeConHash()
	{
		if( !_poConHash ) return;
		delete _poConHash;
		_poConHash = NULL;
	}
	void FreeGroups()
	{
		if( !pvGroups ) return;
		for( size_t i=0;i<pvGroups->size();i++)
		{
			delete (*pvGroups)[i];
			(*pvGroups)[i] = NULL;
		}
		pvGroups->clear();
		delete pvGroups;
		pvGroups = NULL;
	}

	void FreeGroupList4SelfIP()
	{
		if( !pGroupList4SelfIP ) return;
		free( pGroupList4SelfIP );
		pGroupList4SelfIP = NULL;
	}
};

static bool s_bOpenTest = false;

static param s_param0;
static param s_param1;

//static param *s_o = &s_param1;
//static param *s_curr = &s_param0;



static int ReadUintItem(
		CConfig & reader, const char * svrListPath, 
		const char * section, const char * item, uint32_t & result)
{
	ConfigItemInfo_t infoArray [] = 
	{
		CONFIG_ITEM_UIN(section, item, result),
		CONFIG_ITEM_END
	};

	int ret = ConfigRead(&reader, infoArray);
	if(0 != ret)
	{
		logdebug("ERR: %s:%d ConfigRead %s section %s item %s ret %d", __FILE__, __LINE__, svrListPath, section, item, ret);
		return ret;
	}
	ConfigDump(infoArray);
	return 0;
}

static int ReadBatchStrItem(
		CConfig & reader, const char * svrListPath, 
		int count, const char * section, const char * itemPrefix, 
		std::vector<std::string> & results)
{
	char item[32] = {0};
	char value[256] = {0};
	for(int index = 0; index < count; ++index)
	{
		snprintf(item, sizeof(item), "%s%d", itemPrefix, index);
		ConfigItemInfo_t infoArray [] = 
		{
			CONFIG_ITEM_STR(section, item, value),
			CONFIG_ITEM_END
		};

		int ret = ConfigRead(&reader, infoArray);
		if(0 != ret)
		{
			logerr("ERR: %s:%d ConfigRead %s section %s item %s ret %d", __FILE__, __LINE__, 
					svrListPath, section, item, ret);
			return ret;
		}

		results.push_back(value);
		ConfigDump(infoArray);
	}
	return 0;
}

static bool IsVectorContain(
		const std::vector<std::string> & container, const char * target)
{
	for(uint32_t index = 0; index < container.size(); ++index)
	{
		if(container[index] == target)
		{
			return true;
		}
	}
	return false;
}

static void FillSvrAddr(
		const std::vector<std::string> & ip, int port, SvrAddr_t * addrs)
{
	for( uint32_t index = 0; index < ip.size(); ++index )
	{
		addrs[index].iPort = htons(port);

		struct in_addr stIP;  
		inet_aton(ip[index].c_str(), &stIP); //net-order now
		addrs[index].iIP = stIP.s_addr;
	}
}

static int GetLCM(int a, int b)
{
	int iMax = a>b ? a:b;
	for(int i=iMax; i<=a*b; i++)
	{
		if(i%a==0 && i%b == 0)
		{
			return i;
		}
	}

	return -1;
}

static void GenerateFakeIPC(
		uint32_t countAB, uint32_t groupCount, 
        std::vector<std::string> & ipC)
{
	if(countAB == 6 && groupCount == countAB)
	{
		return;
	}

	int iFakeCount = GetLCM(countAB, groupCount);
	int iCCount = ipC.size();
	for(int i=0; i< iFakeCount - iCCount; i++)
	{
		ipC.push_back(ipC[ i % iCCount]);

		logerr("DEBUG: %s:%s:%d push_back index %d %s iFakeCount %u iCCount %u",
				__FILE__, __func__, __LINE__, i % iCCount, ipC[ i % iCCount].c_str(), iFakeCount, iCCount);

	}
	//printf("countAB %u group %u fake %d ccount %d size %u\n", countAB, groupCount, iFakeCount, iCCount, (uint32_t)ipC.size());
}


clsSvrListConfigBase::clsSvrListConfigBase()
	: _grouplist(NULL)
	, _poConHash(NULL)
	, _bOpenTest(false)
	, _old(NULL)
	, _curr(NULL)
{
	memset(_selfIP, 0, sizeof(_selfIP));
	memset(_svrListPath, 0, sizeof(_svrListPath));
	Reset();
}

clsSvrListConfigBase::~clsSvrListConfigBase()
{
	Reset();
	if (NULL != _old)
	{
		delete _old;
		_old = NULL;
	}

	if (NULL != _curr)
	{
		delete _curr;
		_curr = NULL;
	}
}

void clsSvrListConfigBase::Reset()
{
	delete _poConHash;
	_poConHash = NULL;

	_iServerCount = 0;
	_iConsistentHash = 0;
	_iGroupCount = 6;
	_iTimeOut = 500;
	_iOssAttrID = 0;
	_iFailCount = 60;
	_iFailPeriod = 60;

	free(_grouplist);
	_grouplist = NULL;
}


class clsCmpSvrGroup
{
public:
	inline bool operator()( const SvrGroup_t *a,const SvrGroup_t *b ) //for sort by begin
	{
		return a->iBegin < b->iBegin;
	}
	inline bool operator()( const SvrGroup_t *a,uint32_t b ) //for lower_bound
	{
//		printf("a->iEnd %u b %u\n",a->iEnd,b );
		return a->iEnd < b;
	}

};

static SvrGroup_t *GetSvrGroupBySect( std::vector< SvrGroup_t *> &v,uint32_t sect )
{
//	printf("v.size %zu sect %u v[0]->iBegin %u\n",v.size(),sect,v[0]->iBegin );
	if( v.empty() || sect < v[0]->iBegin ) return NULL;

	clsCmpSvrGroup cmp;
    std::vector< SvrGroup_t * >::iterator it = lower_bound( v.begin(),v.end(),sect,cmp );

//	printf("it == end %d\n",it == v.end() );
//	if( it != v.end() )
//	{
//		printf("begin %u end %u  sect %u\n",(*it)->iBegin,(*it)->iEnd,sect );
//	}

	if( it != v.end() && (*it)->iBegin <= sect && sect <= (*it)->iEnd )
	{
		return *it;
	}
	return NULL;

}

static int SortAndCheckOverlapped( std::vector< SvrGroup_t * > & v )
{
	if( v.size() < 2 ) return 0;

	bool need_sort = false;
	for( size_t i=1;i<v.size();i++ )
	{
		if( v[i]->iBegin < v[i - 1]->iBegin )
		{
			need_sort = true;
			break;
		}
	}

	if( need_sort )
	{
		clsCmpSvrGroup cmp;
        std::sort( v.begin(),v.end(),cmp );
	}

	//check overlapped
	for( size_t i=1;i<v.size();i++ )
	{
		if( v[i]->iBegin < v[i-1]->iEnd ) return _err_line;	
	}
	return 0;

}

static int FillSvrGroupList( SvrGroupList_t *grouplist,
							 const  std::vector< SvrGroup_t *> &v,
							 const char *_selfIP )
{
	struct in_addr stIP;  
	inet_aton( _selfIP, &stIP ); //net-order now
	int ip = stIP.s_addr;

	grouplist->iGroupCnt = 0;
	grouplist->iMachineC = 0;

	for( size_t i=0;i<v.size();i++)
	{
		SvrGroup_t &g = *v[i];

		bool ab = false;
		bool c = false;

		//check is ab
		for( size_t j=0;j< sizeof(g.tAddrAB)/sizeof(g.tAddrAB[0]);j++ )
		{
			if( ip == g.tAddrAB[j].iIP )
			{
				ab = true;
				break;
			}
		}
		//check is c
		for( size_t j=0;j< sizeof(g.tAddrC)/sizeof(g.tAddrC[0]);j++ )
		{
			if( ip == g.tAddrC[j].iIP )
			{
				c = true;
				break;
			}
		}
		//fill
		if( ab || c )
		{
			int idx = grouplist->iGroupCnt++;
			if( c )
			{
				grouplist->iMachineC = 1; //true or false
			}
			memcpy( grouplist->tSvrGroup + idx,&g,sizeof(g) ); //WARNING !!! may be out of bound

		}
	}
	return 0;
}

static int LoadConfigFromCConfig_New( param &a,

										uint32_t iOldConsistentHash,
										uint32_t iOldServerCount,
										clsConHash *pOldConHash,

										Comm::CConfig& reader,char *_svrListPath,const char *_selfIP )
{
	//-----------------------------------

    std::vector< SvrGroup_t * > *pvGroups = new std::vector< SvrGroup_t* >();
    std::unique_ptr< std::vector< SvrGroup_t* > > auto_groups( pvGroups );

    std::vector< SvrGroup_t * > &m_vGroups = *pvGroups;

	int ret = 0;

	ret = ReadUintItem( reader, _svrListPath, "General", "GroupCount", a._iGroupCount );
	if( ret < 0 )
	{
		a._iGroupCount = 6;
	}

	if( ReadUintItem(reader, _svrListPath, "General", "FailPeriod", a._iFailPeriod) < 0 || 
		ReadUintItem(reader, _svrListPath, "General", "FailCount", a._iFailCount) < 0 ||
		ReadUintItem(reader, _svrListPath, "General", "TimeOut", a._iTimeOut) < 0 )
	{
		a._iFailPeriod = 60;
		a._iFailCount = 60;
		a._iTimeOut = 500;
	}

	uint32_t iServerCount = 0;
	ret = ReadUintItem( reader, _svrListPath, "Server", "ServerCount", iServerCount );
	if( ret < 0 ) return _err_line;


	ReadUintItem( reader, _svrListPath, "Server", "KvConsistentHash", a._iConsistentHash );

	//server count of conhash changed, clsConHash should be reinit
	if( a._iConsistentHash 
		 &&	( !pOldConHash || !iOldConsistentHash || iOldServerCount != iServerCount ) ) 
	{
		clsConHash * poConHash = new clsConHash();
		int ret = poConHash->Init( iServerCount );
		if( ret < 0 )
		{
			logerr("ERR: clsConHash Init(%d) %d", iServerCount, ret);
			return _err_line;
		}
		
		a._poConHash = poConHash;
		a._iServerCount = iServerCount;
	}

	char section[16] = {0};

	for(uint32_t index = 0; index < iServerCount; ++index)
	{
		snprintf(section, sizeof(section), "Server%d", index);

		uint32_t svrCountAB = 0, svrCountC = 0, port = 0, begin = 0, end = 0;
		{

			int ret = ReadUintItem( reader, _svrListPath, section, "SVRCount", svrCountAB );
			if( ret < 0 ) return _err_line;

			ret = ReadUintItem( reader, _svrListPath, section, "SVR_C_Count", svrCountC );
			if( ret < 0 ) return _err_line;

			ret = ReadUintItem(reader, _svrListPath, section, "SVR_Port", port);
			if( ret < 0 ) return _err_line;

//			printf ( "section %s iPort %d\n", section, port );


			//1.get sect [ begin,end ]
			//
			if( !a._iConsistentHash )
			{
				ret = ReadUintItem( reader, _svrListPath, section, "Sect_Begin", begin );
				if( ret < 0 ) return _err_line;

				ret = ReadUintItem( reader, _svrListPath, section, "Sect_End", end );
				if( ret < 0 ) return _err_line;

				if( begin > end ) return _err_line;
			}
			else
			{
				begin = index;
				end = index;
			}
		}

		//2.get ip
        std::vector<std::string> ipAB, ipC;

		ret = ReadBatchStrItem( reader, _svrListPath, svrCountAB, section, "SVR", ipAB );
		if( ret < 0 ) return _err_line;

		ret = ReadBatchStrItem( reader, _svrListPath, svrCountC, section, "SVR_C", ipC );
		if( ret < 0 ) return _err_line;

		if( svrCountAB != ipAB.size() )
		{
			return _err_line;
		}
		if( svrCountC != ipC.size() )
		{
			return _err_line;
		}
		
		
		//generate fake c ips
		//
		GenerateFakeIPC( svrCountAB, a._iGroupCount, ipC );
		svrCountC = ipC.size();
		
		SvrGroup_t * group = new SvrGroup_t();
        std::unique_ptr< SvrGroup_t > x( group );
		memset( group,0,sizeof(*group) );
		

		{
			group->iCountAB = svrCountAB;
			group->iCountC = svrCountC;
			group->iBegin = begin;
			group->iEnd = end;
			group->iGroupCount = a._iGroupCount;
		}

		//WARNING !!! may be out of bound
		FillSvrAddr( ipAB, port, group->tAddrAB );
		FillSvrAddr( ipC, port, group->tAddrC );

		m_vGroups.push_back( x.release() );
	}

	
	ret = SortAndCheckOverlapped( m_vGroups  );
	if( ret ) return _err_line;

	SvrGroupList_t *grouplist = (SvrGroupList_t*)calloc( sizeof( SvrGroupList_t ),1 );
	{
        std::unique_ptr< SvrGroupList_t > oo( grouplist );
		FillSvrGroupList( grouplist, m_vGroups, _selfIP );
		a.SetGroupList4SelfIP( oo.release() );
	}

	a.SetGroups( auto_groups.release() );

	return 0;
}

int clsSvrListConfigBase::LoadConfigFromCConfig(Comm::CConfig& reader)
{
	int ret = LoadConfigFromCConfigOld(reader);
	logerr("LoadConfigFromCConfigOld ret %d", ret);
	if (0 != ret)
	{
		return ret;
	}

	// only when LoadConfigFromCConfigOld == 0;
	ret = LoadConfigFromCConfigNew(reader);
	logerr("LoadConfigFromCConfigNew ret %d", ret);
	return 0;
}

int clsSvrListConfigBase::LoadConfigFromCConfigNew(Comm::CConfig& reader)
{
	if (false == _bOpenTest)
	{
		return 0; // do nothing;
	}

	assert(NULL != _old);
	assert(NULL != _curr);
	//1. clear old
	if (_old->_poConHash != _curr->_poConHash)
	{
		_old->FreeConHash();
	}

	_old->FreeGroups();
	_old->FreeGroupList4SelfIP();
	//2. load config -> old
	int ret = LoadConfigFromCConfig_New(
			*_old, _old->_iConsistentHash, _old->_iServerCount, 
			_old->_poConHash, reader, 
			_svrListPath, _selfIP);
	if (!ret)
	{
		if (_old->_iConsistentHash && !_old->_poConHash)
		{
			_old->_poConHash = _curr->_poConHash;
		}

		std::swap(_curr, _old);
	}	
	return ret;
}

//int clsSvrListConfigBase::LoadConfigFromCConfigNew(Comm::CConfig& reader)
//{
//	if (false == s_bOpenTest)
//	{
//		return 0;
//	}
//
//		//1. clear old
//		//
////		printf("o->_poConHash %p curr->_poConHash %p\n",
////				s_o->_poConHash,s_curr->_poConHash );
//
//		if( s_o->_poConHash != s_curr->_poConHash )
//		{
//			s_o->FreeConHash();
//		}
//
////		printf("o->pvGroups %p\n",s_o->pvGroups );
//
//		s_o->FreeGroups();
//		s_o->FreeGroupList4SelfIP();
//
//		//2. load config -> old
//		//
//		int ret = LoadConfigFromCConfig_New( *s_o, s_o->_iConsistentHash,
//										s_o->_iServerCount,
//										s_o->_poConHash,
//										reader,
//										_svrListPath,
//										_selfIP );
//
////		printf("ret %d %p\n",ret,s_o->pvGroups );
//		if( !ret )
//		{
//
//			if( s_o->_iConsistentHash && !s_o->_poConHash )
//			{
//				s_o->_poConHash = s_curr->_poConHash;
//			}
//
//			//swap s_curr <-> s_o
////			printf ( "before swap s_o %p s_curr %p\n", s_o, s_curr );
//			param *n = s_curr; s_curr = s_o; s_o = n;
////			printf ( "after swap s_o %p s_curr %p\n", s_o, s_curr );
//
//		}	
//		return ret;
//}

int clsSvrListConfigBase::LoadConfigFromCConfigOld( Comm::CConfig& reader )
{
	// cp from clsNewSvrListConfig::LoadConfg(Comm::CConfig& reader)
	// 1.
	static SvrGroupList_t* pOldGroupList = NULL;
	static clsConHash* pOldConHash = NULL;
	{
		free(pOldGroupList);
		pOldGroupList = NULL;
		free(pOldConHash);
		pOldConHash = NULL;
	}

	if(ReadUintItem(reader, _svrListPath, "General", "GroupCount", _iGroupCount) < 0)
	{
		_iGroupCount = 6;
	}

	if(ReadUintItem(reader, _svrListPath, "General", "FailPeriod", _iFailPeriod) < 0 || 
			ReadUintItem(reader, _svrListPath, "General", "FailCount", _iFailCount) < 0 ||
			ReadUintItem(reader, _svrListPath, "General", "TimeOut", _iTimeOut) < 0)
	{
		_iFailPeriod=60;
		_iFailCount=60;
		_iTimeOut=500;
	}

	uint32_t iServerCount = 0;
	if(ReadUintItem(reader, _svrListPath, "Server", "ServerCount", iServerCount) < 0)
	{
		return -1;
	}

	ReadUintItem(reader, _svrListPath, "Server", "KvConsistentHash", _iConsistentHash);
	//server count of conhash changed, clsConHash should be reinit
	if (_iServerCount != iServerCount && _iConsistentHash)
	{
		clsConHash * poConHash = new clsConHash();
		int ret = poConHash->Init(iServerCount);
		if (ret < 0)
		{
			logerr("ERR: clsConHash Init(%d) %d", iServerCount, ret);
			return -1;
		}
		
		pOldConHash = _poConHash;
		_poConHash = poConHash;

		_iServerCount = iServerCount;
	}
	
	char section[16] = {0};

	SvrGroupList_t * grouplist = (SvrGroupList_t*)calloc(sizeof(SvrGroupList_t), 1);
	bzero(grouplist, sizeof(SvrGroupList_t));
	for(uint32_t index = 0; index < iServerCount; ++index)
	{
		snprintf(section, sizeof(section), "Server%d", index);

		uint32_t svrCountAB = 0, svrCountC = 0, port = 0, begin = 0, end = 0;
		if(ReadUintItem(reader, _svrListPath, section, "SVRCount", svrCountAB) < 0 ||
				ReadUintItem(reader, _svrListPath, section, "SVR_C_Count", svrCountC) < 0 ||
				ReadUintItem(reader, _svrListPath, section, "SVR_Port", port) < 0)
		{
			return -1;
		}
		if (!_iConsistentHash)
		{
			if(ReadUintItem(reader, _svrListPath, section, "Sect_Begin", begin) < 0  ||
					ReadUintItem(reader, _svrListPath, section, "Sect_End", end) < 0) 
			{
				return -1;
			}
		}

        std::vector<std::string> ipAB, ipC;
		if(ReadBatchStrItem(reader, _svrListPath, svrCountAB, section, "SVR", ipAB) < 0 ||
				ReadBatchStrItem(reader, _svrListPath, svrCountC, section, "SVR_C", ipC) < 0)
		{
			return -1;
		}
		bool bContainAB = IsVectorContain(ipAB, _selfIP);
		bool bContainC = IsVectorContain(ipC, _selfIP);
		if(!bContainAB && !bContainC)
		{
			continue;
		}

		//for consistenthash
		if (_iConsistentHash)
		{
			begin = index;
			end = index;
		}
		
		//generate fake c ips
		GenerateFakeIPC(svrCountAB, _iGroupCount, ipC);
		svrCountC = ipC.size();
		
		SvrGroup_t * group = &(grouplist->tSvrGroup[grouplist->iGroupCnt]);
		group->iCountAB = svrCountAB;
		group->iCountC = svrCountC;
		group->iBegin = begin;
		group->iEnd = end;
		group->iGroupCount = _iGroupCount;
		FillSvrAddr(ipAB, port, group->tAddrAB);
		FillSvrAddr(ipC, port, group->tAddrC);

		if(bContainC)
		{
			grouplist->iMachineC = 1;
		}

		grouplist->iGroupCnt++;

		assert(grouplist->iGroupCnt <= MAX_C_SHARE_GROUP);

		if(bContainAB)
		{
			break;
		}
	}

	//no changed
	if(NULL != _grouplist && 0 == memcmp(grouplist, _grouplist, sizeof(SvrGroupList_t)))
	{
		free(grouplist); 
		grouplist = NULL;
		return 0;
	}

	if(grouplist->iGroupCnt == 0)
	{
		return 1;
	}
	else
	{
		pOldGroupList = _grouplist;
		_grouplist = grouplist;
		logerr("\033[33m INFO: %s:%d kvsvr_list.conf changed\033[0m\n", __FILE__, __LINE__);
		return 0;
	}
	
	return -1;
}


void clsSvrListConfigBase::SetSelfIP(const char* sIP)
{
	snprintf(_selfIP, sizeof(_selfIP), "%s", sIP);
}

void clsSvrListConfigBase::SetSvrListPath(const char* sSvrListPath)
{
	snprintf(_svrListPath, sizeof(_svrListPath), "%s", sSvrListPath);
}


void clsSvrListConfigBase::GetSvrGroupList(SvrGroupList_t& grouplist)
{
	if (NULL == _grouplist)
	{
		memset(&grouplist, 0, sizeof(SvrGroupList_t));
		return ;
	}
	assert(NULL != _grouplist);
	grouplist.iGroupCnt = _grouplist->iGroupCnt;
	grouplist.iMachineC = _grouplist->iMachineC;
	memcpy(grouplist.tSvrGroup, _grouplist->tSvrGroup, 
			sizeof(SvrGroup_t) * grouplist.iGroupCnt);
}


clsConHash* clsSvrListConfigBase::GetConHash()
{
	return _poConHash;
}

void clsSvrListConfigBase::PrintSvrList()
{
	printf("SvrGroupList_t %p self %s path %s\n", _grouplist, _selfIP, _svrListPath);

	if(_grouplist == NULL)
	{
		return;
	}

	printf("iGroupCnt %u iMachineC %u\n", _grouplist->iGroupCnt, _grouplist->iMachineC);
	for(int index = 0; index < MAX_C_SHARE_GROUP; ++index)
	{
		printf("index %d\n", index);
		SvrGroup_t & group = _grouplist->tSvrGroup[index];
		printf("\tiCountAB %u iCountC %u iBegin %u iEnd %u iGroupCount %u\n",
				group.iCountAB, group.iCountC, group.iBegin, group.iEnd, group.iGroupCount);
		for(int cursor = 0; cursor < MAX_SVR_COUNT_AB_PER_GROUP; ++cursor)
		{
			printf("\tSVR%d %s\n", cursor, group.tAddrAB[cursor].GetIP());
		}
		for(int cursor = 0; cursor < MAX_SVR_COUNT_C_PER_GROUP; ++cursor)
		{
			printf("\tSVRC%d %s\n", cursor, group.tAddrC[cursor].GetIP());
		}
	}

}


int clsSvrListConfigBase::GetSelfIdx()
{
	if( !_grouplist ) return -1;
	for(size_t i=0;i < sizeof(_grouplist->tSvrGroup) / sizeof(_grouplist->tSvrGroup[0] );i++)
	{
		SvrGroup_t &g = _grouplist->tSvrGroup[i];
		for( size_t j=0;j<sizeof(g.tAddrAB) / sizeof(g.tAddrAB[0]);j++ )
		{
			SvrAddr_t &a = g.tAddrAB[j];
			if( !strcmp( _selfIP,a.GetIP() ) )
			{
				return j;
			}
		}
		for( size_t j=0;j<sizeof(g.tAddrC) / sizeof(g.tAddrC[0]);j++ )
		{
			SvrAddr_t &a = g.tAddrC[j];
			if( !strcmp( _selfIP,a.GetIP() ) )
			{
				return j + g.iCountAB;
			}
		}
	}
	return -1;
}

SvrGroup_t *clsSvrListConfigBase::GetBySect( uint32_t sect )
{
	if (NULL == _curr)
	{
		return NULL;
	}

	assert(NULL != _curr);
	return GetSvrGroupBySect(*(_curr->pvGroups), sect);
//	if( !s_curr ) return NULL;
//	return GetSvrGroupBySect( *s_curr->pvGroups,sect );
}

int clsSvrListConfigBase::GetSvrSetIdx(uint32_t sect)
{
	if (NULL == _curr)
	{
		return -1;
	}

	assert(NULL != _curr);
	const std::vector<SvrGroup_t*>& vecSvrGroup = *(_curr->pvGroups);
	if (true == vecSvrGroup.empty() || sect < vecSvrGroup[0]->iBegin)
	{
		return -2;
	}

	clsCmpSvrGroup cmp;
	std::vector<SvrGroup_t*>::const_iterator 
		iter = std::lower_bound(
				vecSvrGroup.begin(), vecSvrGroup.end(), sect, cmp);
	if (iter == vecSvrGroup.end() || 
			(*iter)->iBegin > sect || (*iter)->iEnd < sect)
	{
		return -3;
	}

	assert(iter != vecSvrGroup.end());
	return iter - vecSvrGroup.begin();
}

int clsSvrListConfigBase::GetAllDataSvr(std::vector<SvrAddr_t>& vecDataSvr)
{
	if (NULL == _curr)
	{
		return -1;
	}

	assert(NULL != _curr);
	vecDataSvr.clear();
    std::vector<SvrGroup_t*>& vecSvrGroup = *(_curr->pvGroups);
	for (size_t i = 0; i < vecSvrGroup.size(); ++i)
	{
		SvrGroup_t* pGrp = vecSvrGroup[i];
		assert(NULL != pGrp);
		vecDataSvr.reserve(vecDataSvr.size() + pGrp->iCountAB);
		for (uint32_t k = 0; k < pGrp->iCountAB; ++k)
		{	
			vecDataSvr.push_back(pGrp->tAddrAB[k]);
		}
	}

	return 0;
//	if (NULL == s_curr)
//	{
//		return -1;
//	}
//
//	vecDataSvr.clear();
//	vector<SvrGroup_t*>& vecSvrGroup = *(s_curr->pvGroups);
//	for (size_t i = 0; i < vecSvrGroup.size(); ++i)
//	{
//		SvrGroup_t* pGrp = vecSvrGroup[i];
//		assert(NULL != pGrp);
//		vecDataSvr.reserve(vecDataSvr.size() + pGrp->iCountAB);
//		for (uint32_t k = 0; k < pGrp->iCountAB; ++k)
//		{	
//			vecDataSvr.push_back(pGrp->tAddrAB[k]);
//		}
//	}
//
//	return 0;
}

void clsSvrListConfigBase::OpenTest()
{
	s_bOpenTest = true;
}

void clsSvrListConfigBase::OpenTestFlag()
{
	if (_bOpenTest)
	{
		assert(NULL != _old);
		assert(NULL != _curr);
		return ;
	}

	_old = new param;
	_curr = new param;
	assert(NULL != _old);
	assert(NULL != _curr);
	_bOpenTest = true;
}



namespace {

void* UpdateConfigThread(void* arg)
{
	BindToLastCpu();
	clsSillySvrListConfig* poConfig = 
		reinterpret_cast<clsSillySvrListConfig*>(arg);
	assert(NULL != poConfig);

	while (true)
	{
		int ret = poConfig->UpdateConfig();
		logerr("UpdateConfig ret %d", ret);
		sleep(1);
	}

	return NULL;
}

} // namespace


clsSillySvrListConfig::clsSillySvrListConfig(const char* sSvrListConf)
{
	assert(NULL != sSvrListConf);
	SetSvrListPath(sSvrListConf);
	// !!!
	OpenTestFlag();
}

int clsSillySvrListConfig::UpdateConfig()
{
	CConfig reader(GetSvrListPath());
	int ret = reader.Init();
	if (0 > ret)
	{
		logerr("ERR: %s[%d] CConfig::Init %s ret %d", 
				__FILE__, __LINE__, GetSvrListPath(), ret);
		return ret;
	}

	LoadConfigFromCConfigOld(reader);
	return LoadConfigFromCConfigNew(reader);
}


