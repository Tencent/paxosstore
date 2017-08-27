
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "iconfig_hash.h"

using namespace std;

namespace Comm {

CStringSlice :: CStringSlice() 
{
	m_pcStart = m_pcEnd = NULL;
}


CStringSlice :: CStringSlice(const char *pcStart, const char *pcEnd) {
	m_pcStart = pcStart;
	m_pcEnd = pcEnd;
}

CStringSlice :: CStringSlice(const std::string & strOther) {
	m_pcStart = strOther.c_str();
	m_pcEnd = strOther.c_str() + strOther.size();
}	

std::string CStringSlice :: ToStr()
{
	std::string ret(m_pcStart, m_pcEnd-m_pcStart);
	return ret;
}

void CStringSlice :: StrTrim(const char *sDelimiter) 
{
	while( m_pcStart < m_pcEnd && strchr(sDelimiter, *m_pcStart) != 0 ) m_pcStart++;
	while ( m_pcStart < m_pcEnd && strchr(sDelimiter, *(m_pcEnd-1)) != 0 ) m_pcEnd--;

}

bool CStringSlice :: operator!=(const CStringSlice &strOther )  const
{
	return ! (*this == strOther) ;
}

bool CStringSlice :: operator==(const CStringSlice &strOther ) const
{
	if ( m_pcEnd - m_pcStart != strOther.m_pcEnd - strOther.m_pcStart ) return false;
	return  strncasecmp(m_pcStart, strOther.m_pcStart, m_pcEnd - m_pcStart) == 0 ;
}

bool CStringSlice :: operator==(const std::string &strOther)  const
{
	if ( m_pcEnd - m_pcStart != (int)strOther.size() ) return false;
	return strncasecmp(m_pcStart, strOther.c_str(), m_pcEnd - m_pcStart) == 0;
}

bool CStringSlice :: operator<(const CStringSlice &strOther) const 
{
	if ( m_pcStart == strOther.m_pcStart ) return false;

	int minlen = m_pcEnd - m_pcStart, res;
	if ( strOther.m_pcEnd - strOther.m_pcStart < minlen ) 
		minlen = strOther.m_pcEnd - strOther.m_pcStart;
	if ( (res = strncasecmp( m_pcStart, strOther.m_pcStart, minlen )) < 0)
		return true;
	else if ( res >0 )
		return false;
	else {
		return m_pcEnd - m_pcStart < strOther.m_pcEnd - strOther.m_pcStart ;
	}	

}


/* ConfigHashNode */
ConfigHashNode :: ConfigHashNode() : m_iHashnum(0),m_iNext(0){ }

ConfigHashNode ::ConfigHashNode(const unsigned int iHashnum, const CStringSlice & sec, const CStringSlice &key, const CStringSlice &val, int iNext)
{
	m_iHashnum = iHashnum;
	m_sec = sec;
	m_key = key;
	m_val = val;
	m_iNext = iNext;
}

unsigned ConfigHashNode ::GetHashnum() 
{
	return m_iHashnum;
}

int ConfigHashNode ::GetNext()
{
	return m_iNext;
}
CStringSlice & ConfigHashNode :: GetSec()
{
	return m_sec;
}
CStringSlice & ConfigHashNode :: GetKey()
{
	return m_key;
}
CStringSlice & ConfigHashNode :: GetVal()
{
	return m_val;
}




/* A simple Vector for ConfigHashTable */

ConfigFakeVector :: ConfigFakeVector() 
{
	iMaxSize = STEP_SIZE;		
	iSize = 0;
	m_aNode = (ConfigHashNode *)malloc(sizeof(ConfigHashNode) * iMaxSize); 
}

ConfigFakeVector :: ConfigFakeVector(ConfigFakeVector & vOther) 
{		
	iSize = vOther.iSize;
	iMaxSize = vOther.iMaxSize;
	ConfigHashNode * p = (ConfigHashNode *)malloc(sizeof( ConfigHashNode )* iMaxSize);
	assert( p );
	m_aNode = p;
	memcpy(m_aNode, vOther.m_aNode, sizeof(ConfigHashNode) * iMaxSize);
}


ConfigFakeVector :: ~ConfigFakeVector() 
{
	if ( NULL != m_aNode )  
	{
		free(m_aNode);
	}
}

void ConfigFakeVector :: Clear() 
{
	iSize = 0;
}

unsigned int ConfigFakeVector :: GetSize()
{
	return iSize;
}

void ConfigFakeVector :: Add(ConfigHashNode node) 
{
	if ( iSize == iMaxSize )	{
		iMaxSize += STEP_SIZE;
		ConfigHashNode * p = (ConfigHashNode*) realloc(m_aNode, iMaxSize * sizeof(ConfigHashNode));
		assert( p );
		m_aNode = p;
	}
	m_aNode[iSize++] = node;
}

ConfigHashNode & ConfigFakeVector :: operator[](int num) const
{
	return m_aNode[num];	
}

/* ConfigHashTable */

unsigned int ConfigHashTable :: HASH_SIZE = ConfigHashTable :: DEFAULT_HASH_SIZE;

ConfigHashTable :: ConfigHashTable() {
	m_aIndex = NULL;
	Init();
}

ConfigHashTable :: ConfigHashTable(int iHashSize)
{
	if ( (iHashSize & (-iHashSize )) == iHashSize )/*changed by calvinzang 2012/7/30 */
		HASH_SIZE = iHashSize;
	else {
		HASH_SIZE = 1;
		while( HASH_SIZE < (unsigned int)iHashSize )
			HASH_SIZE <<= 1;
	}
	m_aIndex = NULL;
}

ConfigHashTable :: ConfigHashTable(ConfigHashTable &other) 
{
	m_aIndex = (int*)malloc(HASH_SIZE * sizeof(int));
    assert(NULL != m_aIndex);

	memcpy(m_aIndex, other.m_aIndex, sizeof(int) * HASH_SIZE);
	m_table = other.m_table;
}

ConfigHashTable :: ~ConfigHashTable() 
{
	if ( NULL != m_aIndex )
	{
		free( m_aIndex );
	}
}

void ConfigHashTable :: Init() 
{
	m_aIndex = (int*)malloc(HASH_SIZE * sizeof(int));
    assert(NULL != m_aIndex);
	memset( m_aIndex, 255, sizeof(int) * HASH_SIZE );		
}

void ConfigHashTable :: Clear() 
{
	memset( m_aIndex, 255, sizeof(int) * HASH_SIZE );	
	m_table.Clear();
}

//using FNV1 hash function
inline unsigned int ConfigHashTable :: HashFun(const CStringSlice &sec, const CStringSlice &key)
{
	static const unsigned int FNV1_BASIS = 2166136261lu;
	static const unsigned int FNV1_PRIME = 16777619; 
	unsigned int ret = FNV1_BASIS;
	const char *ptr, *endptr;
	endptr = sec.GetEnd();

	for( ptr=sec.GetStart(); ptr < endptr; ptr++ )
	{
		ret *= FNV1_PRIME;
		ret ^= tolower(*ptr);
	}
	endptr = key.GetEnd();

	for( ptr=key.GetStart(); ptr < endptr; ptr++ )
	{
		ret *= FNV1_PRIME;
		ret ^= tolower(*ptr);
	}

	return ret ;		
}

void ConfigHashTable :: Add(const CStringSlice &sec,const CStringSlice &key, const CStringSlice &val)
{
	if ( NULL == m_aIndex ) Init();

	unsigned int hashnum = HashFun(sec, key);
	unsigned int bucket = hashnum & (HASH_SIZE - 1);	

	m_table.Add(ConfigHashNode(hashnum, sec, key, val, m_aIndex[bucket]));
	m_aIndex[bucket] = m_table.GetSize() - 1;
}

CStringSlice ConfigHashTable :: Get(const CStringSlice &sec, const CStringSlice &key)
{
	unsigned int hashnum = HashFun(sec, key);
	unsigned int bucket = hashnum & ( HASH_SIZE- 1 );

	for ( int i=m_aIndex[bucket]; i!= -1; i=m_table[i].GetNext() )
	{
		if ( m_table[i].GetHashnum() == hashnum && 
				m_table[i].GetSec() == sec && m_table[i].GetKey() == key )
		{
			return m_table[i].GetVal();
		}
	}
	return CStringSlice();
}

bool ConfigHashTable :: Empty(  )	
{
	return m_table.GetSize() == 0;
}

}

