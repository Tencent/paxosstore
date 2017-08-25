
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include<vector>
#include<string>

namespace Comm 
{

class CStringSlice;
class ConfigFakeVector;
class ConfigHashNode;
class ConfigHashTable;

/*!
 * \brief	locate a paticular slice of a string using two pointers, 
 * 			other than copying to another memory space.
 * \note    the string must be accessed in memory.
 * 			It's not advisable to use this class unless you test how to.           
 * \as	testIConfigHashCStringSlice.cpp
 */
/* Store index of string */
class CStringSlice {

	public:

	
	/*!
	 * \brief	constructor ³õÊ¼»¯Ò»¸ö¿Õ×Ö·û´®
	 */
	CStringSlice(); 

	/*!
	 * \brief	¸ù¾ÝÒ»¸ö×Ö·û´®µÄÊ×Î²Ö¸Õë¹¹Ôì¶ÔÏó
	 *
	 * \param	pcStart ×Ö·û´®µÄÍ·Ö¸Õë
	 * \param	pcEnd ×Ö·û´®µÄÎ²Ö¸Õë
	 */
	CStringSlice(const char *pcStart, const char *pcEnd);

	/*!
	 * \brief	¸ù¾ÝÉÏstring¶ÔÏó¹¹ÔìCSTringSlice¶ÔÏó
	 *
	 * \param	strOther
	 */
	CStringSlice(const std::string & strOther) ;	

	/*!
	 * \brief	ÒÔ string ÀàÐÍ·µ»Ø CStringSliceÖ ÐµÄÄÚÈÝ
	 *
	 * \return  string ×Ö·û´®
	 */
	std::string ToStr();

	/*!
	 * \brief Ïû³ýÊ×Î²µÄ sDelimiter ×Ö·û
	 *
	 * \param	sDelimiter ÒªÈ¥³ýµÄ×Ö·û
	 */
	void StrTrim(const char *sDelimiter);

	/*!
	 * \brief	µÃµ½×Ö·û´®µÄÍ·Ö¸Õë
	 *
	 * \return ×Ö·û´®µÄÍ·Ö¸Õë	
	 */
	inline const char *GetStart() const { return m_pcStart; }
	/*!
	 * \brief	µÃµ½×Ö·û´®µÄÎ²Ö¸Õë
	 *
	 * \return	×Ö·û´®µÄÎ²Ö¸Õë
	 */
	inline const char *GetEnd() const { return m_pcEnd; }

	/*!
	 * \brief	±È½ÏÁ½¸ö CStringSlice ¶ÔÏóÊÇ·ñ²»µÈ
	 *
	 * \param	strOther ÁíÒ»¸öÒª±È½ÏµÄ¶ÔÏó
	 *
	 * \retval true not equal
	 * \retval false  equal
	 */
	bool operator!=(const CStringSlice &strOther ) const; 

	/*!
	 * \brief	±È½ÏÁ½¸ö CStringSlice ¶ÔÏóÊÇ·ñÏàµÈ
	 *
	 * \param	strOther
	 *
	 * \retval true equal
	 * \retval false not equal	
	 */
	bool operator==(const CStringSlice &strOther ) const ; 

	/*!
	 * \brief	string ºÍ CStringSlice ¶ÔÏóÊÇ·ñÏàµÈ
	 *
	 * \param	strOther ÁíÍâÒ»¸öÒª±È½ÏµÄ¶ÔÏó
	 *
	 * \retval true equal
	 * \retval false not equal	
	 */
	bool operator==(const std::string &strOther) const; 

	/*!
	 * \brief	±È½ÏÁ½¸öCStringSliceµÄ´óÐ¡
	 *
	 * \param	strOther
	 *
	 * \retval	true Ð¡ÓÚstrOhter
	 * \retval  false ²»Ð¡ÓÚstrOther
	 */
	bool operator<(const CStringSlice &strOther) const ;
	
	private:
	const char *m_pcStart, *m_pcEnd;

};

/*!
 * \brief	the infomation unit to be used by  ConfigHashTable
 * \note	comprehend the usage of m_iNext ,as a back pointer to 
 * 			compose a dense storage hash table.
 * \as testConfigHashNode.cpp
 */
class ConfigHashNode {
	public:
	
	/*!
	 *\brief constructor 
	 *\note this constructor do nothing,it 
	 * does not initiallize any class members.
	 * so be careful to use it
	 * 
	 */
	ConfigHashNode(); 
	
	/*! constructor */
	ConfigHashNode(const unsigned int iHashnum, const CStringSlice & sec, const CStringSlice &key, const CStringSlice &val, int iNext);
	
	
	/*!
	 * \brief get m_iHashnum
	 *
	 * \return m_iHashnum	
	 * \as testConfigHashNode.cpp
	 */
	inline unsigned GetHashnum();
	
	/*!
	 * \brief »ñÈ¡Àà³ÉÔ±m_iNext
	 *
	 * \return	m_iNext
	 * \as testConfigHashNode.cpp
	 */
	inline int GetNext();
	
	/*!
	 * \brief	»ñÈ¡Àà³ÉÔ±m_sec
	 *
	 * \return	m_sec
	 * \as testConfigHashNode.cpp
	 */
	inline CStringSlice & GetSec();
	
	/*!
	 * \brief	»ñÈ¡Àà³ÉÔ±m_key
	 *
	 * \return	m_key
	 * \as testConfigHashNode.cpp
	 */
	inline CStringSlice & GetKey();
	
	/*!
	 * \brief	»ñÈ¡Àà³ÉÔ±m_val
	 *
	 * \return	m_val
	 * \as testConfigHashNode.cpp
	 */
	inline CStringSlice & GetVal();

	private:

	unsigned int m_iHashnum;
	CStringSlice m_sec, m_key, m_val;
	int m_iNext;

};



/*!
 * \brief	A simple vector for ConfigHashTable, each unit is a ConfigHashNode.
 * 			the memory size is auto-increased by Add if necessary.
 * \note    only used for ConfigHashTable.
 * as testConfigFakeVector.cpp
 */
/* A simple Vector for ConfigHashTable */
class ConfigFakeVector {
	
public:
	
	/*!
	 * \brief constructor
	 */
	ConfigFakeVector();

	/*!
	 *\brief copy  constructor
	 */
	ConfigFakeVector(ConfigFakeVector & vOther);
	
	/*!
	 * \brief destructor
	 */
	~ConfigFakeVector();


	/*!
	 * \brief 
	 * \note do not free ConfigHashNode
	 */
	void Clear(); 

	inline unsigned int GetSize();

	void Add(ConfigHashNode node); 

	inline ConfigHashNode &operator[](int num) const;
private:

	ConfigHashNode * m_aNode;

	static const unsigned int STEP_SIZE = 128;

	unsigned int iSize, iMaxSize;
	
};


/*!
 * \brief	a dense storage hash table, using FNV1 hash function, CStringSlice section 
 * 			and CStringSlice key as	hash key, CStringSlice val as hash record.
 * \as	testIConfigHashConfigHashTable.cpp
 */
/* The Hash Table */
class ConfigHashTable {

public:

	ConfigHashTable();
	
	/*!
	 * \brief	construct ConfigHashTable, initialized HASH_SIZE according to iHashSize.
	 * 			HASH_SIZE = 2^n && HASH_SIZE >= iHashSize
	 * \note	bugfixed if ( iHashSize & (-iHashSize ) == iHashSize ) into 
	 * 					 if ( (iHashSize & (-iHashSize )) == iHashSize ), test whether iHashSize
	 * 			is 2^k. 
	 * \param	iHashSize int size to initialize HASH_SIZE.
	 */
	ConfigHashTable(int iHashSize);

	/*!
	 * \brief	constructor
	 * \param	other ConfigHashTable
	 */
	ConfigHashTable(ConfigHashTable &other);

	~ConfigHashTable();


	/*!
	 * \brief	allocate index array, initialized -1.
	 */
	void Init();

	/*!
	 * \brief	set each unit in index array to -1; release ConfigFakeVector
	 */
	void Clear();


	/*!
	 * \brief	add into ConfigHashTable
	 * \param	sec  CStringSlice, INI file section part
	 * \param	key	 CStringSlice, INI file key part
	 * \param	val  CStringSlice, INI file value part
	 */
	void Add(const CStringSlice &sec,const CStringSlice &key, const CStringSlice &val);
	
	/*!
	 * \brief	get the CStringSlice record specified by two parameters.
	 * \param	sec CStringSlice, INI file section part
	 * \param	key CStringSlice, INI file key part
	 * \return	CStringSlice, INI file value part if hit; else, null CStringSlice 
	 */
	CStringSlice Get(const CStringSlice &sec, const CStringSlice &key);

	/*!
	 * \brief	test whether ConfigHashTable is empty.
	 * \return true or false.
	 */
	bool Empty();

private:

	//using FNV1 hash function
	inline unsigned int HashFun(const CStringSlice &sec, const CStringSlice &key);


	int * m_aIndex;
	ConfigFakeVector m_table;

	//hash_size *must* be power of 2, ie: 2, 4, 8, 16, 32...
	static const unsigned int DEFAULT_HASH_SIZE = 1024;

	static unsigned int HASH_SIZE;
};



}
