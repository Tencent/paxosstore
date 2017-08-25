
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



//Config.hh define common config 
//read and write interface.
#pragma once

#include <string>
#include <map>
#include <vector>
#include <limits.h>

/**
 *\file Config.hh
 *\brief define common config 
 * read and write interface.
 */

namespace Comm {

#define CONFIG_TYPE_NON 0
#define CONFIG_TYPE_STR 1
#define CONFIG_TYPE_INT 2
#define CONFIG_TYPE_UIN 3
#define CONFIG_TYPE_HEX 4 
#define CONFIG_TYPE_STDSTR 5 

/*!
 * \brief this struct is used  to save ini file's data
 * \as	testConfigRead.cpp
 */
typedef struct tagConfigItemInfo {
	const char * sSection;
	const char * sKey;
	int  iType;
	void * pvAddr;
	int  iSize;
} ConfigItemInfo_t;

#ifndef CONFIG_ITEM_STR
#define CONFIG_ITEM_STR(section, key, var) \
	{ section, key, CONFIG_TYPE_STR, (void*)var, sizeof( var ) }
#endif

#ifndef CONFIG_ITEM_INT
#define CONFIG_ITEM_INT(section, key, var) \
	{ section, key, CONFIG_TYPE_INT, (void*)&(var), sizeof( var ) }
#endif

#ifndef CONFIG_ITEM_UIN
#define CONFIG_ITEM_UIN(section, key, var) \
	{ section, key, CONFIG_TYPE_UIN, &(var), sizeof( var ) }
#endif

#ifndef CONFIG_ITEM_HEX
#define CONFIG_ITEM_HEX(section, key, var) \
	{ section, key, CONFIG_TYPE_HEX, &(var), sizeof( var ) }
#endif

#ifndef CONFIG_ITEM_STDSTR
#define CONFIG_ITEM_STDSTR(section, key, var) \
	{ section, key, CONFIG_TYPE_STDSTR, &(var), 0 }
#endif

#ifndef CONFIG_ITEM_END
#define CONFIG_ITEM_END \
	{ "", "", CONFIG_TYPE_NON, NULL, 0 }
#endif

/*!
 * \brief	this struct is used to save ini file's data
 * \note if the ini file do not have the  corresponding
 *   item,then use sDefault as default value
 *  
 * \as	testConfigRead_ext.cpp
 */
typedef struct tagConfigItemInfoEx {
	const char * sSection;
	const char * sKey;
	int  iType;
	void * pvAddr;
	int  iSize;
	const char * sDefault;
} ConfigItemInfoEx_t;

#define CONFIG_ITEM_EX_STR(section, key, var, defval) \
	{ section, key, CONFIG_TYPE_STR, (void*)var, sizeof( var ), defval }

#define CONFIG_ITEM_EX_INT(section, key, var, defval) \
	{ section, key, CONFIG_TYPE_INT, (void*)&(var), sizeof( var ), defval }

#define CONFIG_ITEM_EX_UIN(section, key, var, defval) \
	{ section, key, CONFIG_TYPE_UIN, &(var), sizeof( var ), defval }

#define CONFIG_ITEM_EX_HEX(section, key, var, defval) \
	{ section, key, CONFIG_TYPE_HEX, &(var), sizeof( var ), defval }

#define CONFIG_ITEM_EX_STDSTR(section, key, var, defval) \
	{ section, key, CONFIG_TYPE_STDSTR, &(var), 0, defval}

#define CONFIG_ITEM_EX_END \
	{ "", "", CONFIG_TYPE_NON, NULL, 0, NULL }


class CConfig;
class CConfigImpl;


/*!
 * \brief	读取配置文件到内存
 *
 * \config 配置文件
 * \infoArray 配置文件保存的地方
 *
 * \retval 0 成功读取
 * \retval -1 有些配置项不存在，可以通过查看日志知道,哪些日志项读取失败
 * \as testConfigRead.cpp
 */
extern int ConfigRead( CConfig * config, ConfigItemInfo_t * infoArray );

extern void ConfigDump( ConfigItemInfo_t * infoArray );

/*!
 * \brief	读取配置文件到内存
 *
 * \ config 配置文件
 * \ infoArray 配置文件保存的地方
 *
 * \retval 0 成功读取
 * \retval -1 有些配置项不存在，可以通过查看日志知道,哪些日志项读取失败
 * \as testConfigRead_ext.cpp
 */
extern int ConfigRead( CConfig * config, ConfigItemInfoEx_t * infoArray );

extern int ConfigRead( CConfig * config, ConfigItemInfoEx_t * infoArray, std::vector<std::string> * suffixList );

extern void ConfigDump( ConfigItemInfoEx_t * infoArray );

/**
 * \brief CConfig is used to read and write ini file format config file.
 * \note you can use ConfigRead(CConfig*, ConfigItemInfo *) to obtain 
 * data from CConfig's object  
 *
 * \as testCConfig.cpp 
 */

// 把configimpl抽离出cpp文件，以后对于基础库读取配置使用CConfigImpl来读取
// ConfigImpl类只实现纯粹的配置读取，派生的业务逻辑在CConfig中实现,如上报读配置文件到mmdata中

class CConfig
{
public:

	CConfig();

	CConfig & operator= (const CConfig& file);

	explicit  inline CConfig(const CConfig& file)
	{ operator=(file); }

	/**
	 * Constructor
	 */       
	CConfig(const std::string& configfile);

	/**
	 * Desctructor
	 */
	virtual  ~CConfig(void);

	const std::string & getConfigFile();


	/**
	 *
	 * read conf log report
	 *
	 */
	  
	void LogReadConfigFile(const std::string& configfile) ;

	/**
	 * \brief Set the ConfigFileName
	 * Used when CConfig use trivial constructor
	 * \configile config file name
	 * \as testCConfig.cpp
	 */

	void SetConfigFile(const std::string &configfile);
	
	/**
	 * \brief Used to get all section title.
	 * \sectionlist indicates section title.
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int getSectionList(
		std::vector<std::string>& sectionlist
	);

	/**
	 * \brief  Used to get a section .
	 * \name indicates section name.
	 * \section indicates section value.
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 *
	 * \as testCConfig.cpp
	 */
	int getSection(
		const std::string& name,
		std::map<std::string,std::string>& section
	);

	int getSection(
		const std::string& name,
		std::vector<std::string> &section
	);

	/**
	 * \brief Used to get a section .
	 * \name indicates section name.
	 * \section indicates section value.
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int getSectionText(
		const std::string& name,
		std::string & sectionText
	);

	/**
	 * \brief Used to Read config Item.
	 * \section indicates config section.
	 * \key indicates config item key.
	 * \defaultvalue indicates value which are
	 *      are supplied by user as default value, if
	 *      fail to get value.
	 * \itemvalue  indicates value which get from 
	 *       config file.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadItem(
		const std::string& section,
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue
	);

    int ReadItem(
		const std::string& section,
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue,
        std::vector<std::string> * suffixList
	);


	/**
	 * \brief Used to Read config Item.
	 * \section indicates config section.
	 * \key indicates config item key.
	 * \defaultvalue indicates value which are
	 *      are supplied by user as default value, if
	 *      fail to get value.
	 * \itemvalue  indicates value which get from 
	 *       config file.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadItem(
		const std::string& section,
		const std::string& key,
		int defaultvalue,
		int& itemvalue
	);

    int ReadItem(
		const std::string& section,
		const std::string& key,
		int defaultvalue,
		int& itemvalue,
        std::vector<std::string> * suffixList
	);

	
	/**
	 * \brief Used to Read config Item.
	 * \section indicates config section.
	 * \key indicates config item key.
	 * \defaultvalue indicates value which are
	 *      are supplied by user as default value, if
	 *      fail to get value.
	 * \itemvalue  indicates value which get from 
	 *       config file.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadItem(
		const std::string& section,
		const std::string& key,
		unsigned int defaultvalue,
		unsigned int& itemvalue
	);

    int ReadItem(
		const std::string& section,
		const std::string& key,
		unsigned int defaultvalue,
		unsigned int& itemvalue,
        std::vector<std::string> * suffixList
	);


	/**
	 * \brief Used to Read config Item, not remove trailing comment
	 *
	 * \section indicates config section.
	 * \key indicates config item key.
	 * \defaultvalue indicates value which are
	 *      are supplied by user as default value, if
	 *      fail to get value.
	 * \itemvalue  indicates value which get from 
	 *       config file.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadRawItem(
		const std::string& section,
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue
	);

	/**
	 * \brief Used to Read config Item, not remove trailing comment
	 *
	 * \section indicates config section.
	 * \key indicates config item key.
	 * \defaultvalue indicates value which are supplied by user as default value, if fail to get value.
	 * \itemvalue  indicates value which get from 
	 *       config file.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadRawItem(
		const std::string& section,
		const std::string& key,
		int defaultvalue,
		int& itemvalue
	);

	/**
	 * \brief Used to Read un-sectioned config Item.
	 *
	 * \key indicates config item key.
	 * \itemvalue  indicates value which get from config file.
	 * \defaultvalue indicates value which are supplied by user as default value, if fail to get value.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadItem(
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue
	);

    int ReadItem(
		const std::string& key,
		const std::string& defaultvalue,
		std::string& itemvalue,
        std::vector<std::string> * suffixList
	);


	/**
	 * \brief Used to Read un-sectioned config Item.
	 *
	 * \key indicates config item key.
	 * \itemvalue  indicates value which get from config file.
	 * \defaultvalue indicates value which are supplied by user as default value, if fail to get value.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadItem(
		const std::string& key,
		int defaultvalue,
		int& itemvalue
	);
    int ReadItem(
		const std::string& key,
		int defaultvalue,
		int& itemvalue,
        std::vector<std::string> * suffixList
	);

	/**
	 *
	 *\brief new interface addby junechen
	 * Used to Read un-sectioned config Item.
	 *
	 * \key indicates config item key.
	 * \itemvalue  indicates value which get from config file.
	 * \defaultvalue indicates value which are supplied by user as default value, if fail to get value.
	 * \retval 0 on success. 
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int ReadItem(
		const std::string& key,
		unsigned int defaultvalue,
		unsigned int & itemvalue
	);
    int ReadItem(
		const std::string& key,
		unsigned int defaultvalue,
		unsigned int & itemvalue,
        std::vector<std::string> * suffixList
	);

	/** 
	 * \brief Only add the item in the memory, so you can add some items,
	 *   then call WriteFile once to save all the new items into the file.
	 *
	 * \section indicates  config section.
	 * \key config item key.
	 * \value config item value.
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	int AddItem(
		const std::string& section,
		const std::string& key,
		const std::string& value
	);

	/**
	 * \brief This function is used to load
	 * 		config file to buf.
	 * \retval 0 on success.
	 * \retval -1 on failure.\as testCConfig.cpp
	 */
	int Init(void);


	//init from a text buff
	int Init(const std::string& text);

	/**
	 * \brief This function is used to dump	iternal config information.
	 * \as testCConfig.cpp
	 */
	void dumpinfo(void);

	/**
	 * \brief This function is used to 
	 * trim \t empty string at string 
	 * start or end. 
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	static int TrimString ( std::string & str );

	/**
	 * \brief This function is used to 
	 * 		trim \t empty string at string 
	 * 		start or end. 
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	static int TrimCStr( char * src );

	
	/**
	 * \brief This function is used split string by delim 
	 * \str 需要分割的字符串
	 * \delim 分隔符
	 * \output 分割结果
	 * \retval 0 on success.
	 * \retval -1 on failure.
	 * \as testCConfig.cpp
	 */
	static void Split(
		const std::string& str,
		const std::string& delim,
		std::vector<std::string>& output
	);
 		
	/*!
	 * \brief get the content of config file
	 *
	 * \return	the content of ini file 
	 * \as testCConfig.cpp
	 */
	std::string GetConfigContent(void);
	
private:

	// 对 CConfig 做了 pimpl 手法的优化，为了避免没有重编译所有代码导致程序 coredump，
	// 对 CConfig 进行填充，使得 CConfig 在 32/64 平台的大小不变。
#if __WORDSIZE == 64
	// for 64bit platform, OLD_SIZE: 64, ( void * ): m_pImpl, ( void * ): vtbl_ptr
	char m_sPadding[  64 - sizeof( void * ) * 2 ];
#else
	// for 32bit platform, OLD_SIZE: 32, ( void * ): m_pImpl, ( void * ): vtbl_ptr
	char m_sPadding[  32 - sizeof( void * ) * 2 ];
#endif

	CConfigImpl *m_pImpl;
};


}

