
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#ifndef COMM2_LOCAL_USE
#error THIS FILE IS USED IN COMM2 ONLY! PLEASE USE iConfig.h INSTEAD
#endif
#pragma once
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>
#include <set>
#include <map>
#include <errno.h>
#include <stdio.h>
#include "iconfig_hash.h"

namespace Comm {
class CConfigImpl {
	
public:
	CConfigImpl();

	~CConfigImpl();

	CConfigImpl & operator= (const CConfigImpl & file);

	CConfigImpl(const CConfigImpl & other)
	{ operator=(other); }

	CConfigImpl(const std::string& configfile);

	const std::string & getConfigFile();

	void SetConfigFile(const std::string &configfile);

	int getSectionList(
		std::vector<std::string>& sectionlist
	);

	int getSection(
		const std::string& name,
		std::map<std::string,std::string>& section
	);

	int getSection(
		const std::string& name,
		std::vector<std::string> &section
	);

	int getSectionText(
		const std::string& name,
		std::string & sectionText
	);

	int ReadItem(
		const std::string& section,
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue
	);

    int ReadItem( const std::string& section,
		const std::string& key,
        const std::string& defaultvalue,
		std::string& itemvalue,
        std::vector<std::string> * suffixList
    );

	int AddItem(
		const std::string& section,
		const std::string& key,
		const std::string& value
	);

	int ReadRawItem(
		const std::string& section,
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue
	);

	int Init(void);
	int Init(const std::string& text);

	void dumpinfo(void);

	std::string GetConfigContent(void);

	static int TrimString ( std::string & str );

	static int TrimCStr( char * src );

	static void Split(
		const std::string& str,
		const std::string& delim,
		std::vector<std::string>& output
	);
 		
private:
	
	/* core/utils/StrLower is slow */
	void StrToLower(std::string &str);

	//void StrToLower(CStringSlice &str);	

	/* Parse buf to hash */
	int ParserConfig( const char *src, int filelen );

	int LoadFile(void);
	int LoadText(const std::string& text);
  
	const char * GetBuffer();

	/**
	 * find the begin of the specify key
	 *
	 * @param src : file content
	 * @param section : section name
	 * @param key : key name
	 *
	 * @param size : the length of the line
	 *        size > 0 : the key is existence
	 *        size = 0 : the key is not existence
	 *
	 * @return (*size) > 0 : the begin of the line
	 * @return (*size) = 0 : the begin for insert new key
	 *
	 */
	static const char * GetItemPos(
		const char * src,
		const char * section,
		const char * key,
		int * size
	);

	/******** variables *******/

	bool m_bIsInited ;

	/* config file name. */
	std::string m_config; 

	/* config buffer */
	std::string m_config_buf;

	std::vector<CStringSlice> m_sectionList;
	ConfigHashTable m_hash;

};
}

