
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



// Config.hh
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>
#include <set>
#include <errno.h>
#include <stdio.h>

#include "cutils/log_utils.h"
#include "iconfig_hash.h"
#include "iconfig.h"
// #include "../iLog.h"
//#include "iConfigLog.h"
//#include "logid_base.h"
// #include "iUtils.h"
#define COMM2_LOCAL_USE
#include "iconfig_impl.h"

using namespace std;
#define MAX_LINE_LEN  1024*32

#define CGIMAGIC_UN_SECTIONED "CGIMAGIC-UN-SECTIONED"
#define CGIMAGIC_SECTION_TEXT "CGIMAGIC-SECTION-TEXT"

#define CGIMAGIC_SECTION_BUFFER "CGIMAGIC-SECTION-BUFFER"
#define CGIMAGIC_KEY_BUFFER     "CGIMAGIC-KEY-BUFFER"

namespace Comm{

__thread void* g_setConfig = NULL;

int ConfigRead( CConfig * config, ConfigItemInfo_t * infoArray )
{
	int ret = 0;

	for( int i = 0; ; ++i )
	{
		ConfigItemInfo_t item = infoArray[i];

		if( NULL == item.pvAddr ) break;

		string value;

		int readRet = 0;

		if( '\0' == item.sSection[0] )
		{
			readRet = config->ReadItem( item.sKey, "", value );
		} else {
			readRet = config->ReadItem( item.sSection, item.sKey, "", value );
		}

		if( 0 != readRet )
		{
			ret = -1;
			logerr( "ConfigRead: [%s]%s is not exist",
					item.sSection, item.sKey );
		}

		if( CONFIG_TYPE_STR == item.iType )
		{
			memset( item.pvAddr, 0, item.iSize );
			strncpy( (char*)item.pvAddr, value.c_str(), item.iSize );
		} else if( CONFIG_TYPE_INT == item.iType ) {
            if(item.iSize == sizeof(char)) {
                *(char*)item.pvAddr = (char)atoi( value.c_str() );
            } else if(item.iSize == sizeof(short)) {
                *(short*)item.pvAddr = (short)atoi( value.c_str() );
            } else if(item.iSize == sizeof(int)) {
                *(int*)item.pvAddr = atoi( value.c_str() );
            } else if(item.iSize == sizeof(long long)) {
                *(long long*)item.pvAddr = atoll( value.c_str() );
            }
		} else if( CONFIG_TYPE_UIN == item.iType ) {
            if(item.iSize == sizeof(unsigned char)) {
                *(unsigned char*)item.pvAddr = (unsigned char)strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned short)) {
                *(unsigned short*)item.pvAddr = (unsigned char)strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned int)) {
                *(unsigned int*)item.pvAddr = strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned long long)) {
                *(unsigned long long*)item.pvAddr = strtoull( value.c_str(), NULL, 10 );
            }
		} else if( CONFIG_TYPE_HEX == item.iType ) {
			*(unsigned int*)item.pvAddr = strtoul( value.c_str(), NULL, 16 );
		} else if( CONFIG_TYPE_STDSTR == item.iType ) {
			*(std::string*)item.pvAddr = value;
		} else {
			logerr( "ConfigRead: unknown type %d, [%s]%s",
					item.iType, item.sSection, item.sKey );
		}
	}

	return ret;
}

void ConfigDump( ConfigItemInfo_t * infoArray )
{
	map< string, vector<ConfigItemInfo_t> > dumpMap;

	for( int i = 0; ; ++i )
	{
		ConfigItemInfo_t item = infoArray[i];

		if( NULL == item.pvAddr ) break;

		dumpMap[ item.sSection ].push_back( item );
	}

	map< string, vector<ConfigItemInfo_t> >::iterator iter = dumpMap.begin();

	for( ; dumpMap.end() != iter; ++iter )
	{
        logerr("INIT: [%s]", iter->first.c_str());

		vector<ConfigItemInfo_t> & list = iter->second;

		for( unsigned int i = 0; i < list.size(); ++i )
		{
			ConfigItemInfo_t & item = list[i];

			if( CONFIG_TYPE_STR == item.iType )
			{
                logerr("INIT: %s = %s", item.sKey, (char*)item.pvAddr);
			}

			if( CONFIG_TYPE_INT == item.iType )
			{
                if(item.iSize == sizeof(char)) {
                    logerr("INIT: %s = %d",
                            item.sKey, *(char*)item.pvAddr );
                } else if(item.iSize == sizeof(short)) {
                    logerr("INIT: %s = %d",
                            item.sKey, *(short*)item.pvAddr );
                } else if(item.iSize == sizeof(int)) {
                    logerr("INIT: %s = %d",
                            item.sKey, *(int*)item.pvAddr );
                } else if(item.iSize == sizeof(long long)) {
                    logerr("INIT: %s = %lld",
                            item.sKey, *(long long*)item.pvAddr );
                }
            }

            if( CONFIG_TYPE_UIN == item.iType )
			{
                if(item.iSize == sizeof(unsigned char)) {
                    logerr("INIT: %s = %u",
                            item.sKey, *(unsigned char*)item.pvAddr );
                } else if(item.iSize == sizeof(unsigned short)) {
                    logerr("INIT: %s = %u",
                            item.sKey, *(unsigned short*)item.pvAddr );
                } else if(item.iSize == sizeof(unsigned int)) {
                    logerr("INIT: %s = %u",
                            item.sKey, *(unsigned int*)item.pvAddr );
                } else if(item.iSize == sizeof(unsigned long long)) {
                    logerr("INIT: %s = %llu",
                            item.sKey, *(unsigned long long*)item.pvAddr );
                }

            }
            if( CONFIG_TYPE_STDSTR == item.iType ) {
                logerr("INIT: %s = %s",
                        item.sKey, ((std::string*)item.pvAddr)->c_str() );
        }
    }
    }
}

int ConfigRead( CConfig * config, ConfigItemInfoEx_t * infoArray )
{
	int ret = 0;

	for( int i = 0; ; ++i )
	{
		ConfigItemInfoEx_t item = infoArray[i];

		if( NULL == item.pvAddr ) break;

		string value;

		int readRet = 0;

		if( '\0' == item.sSection[0] )
		{
			readRet = config->ReadItem( item.sKey, "", value );
		} else {
			readRet = config->ReadItem( item.sSection, item.sKey, "", value );
		}

		if( 0 != readRet )
		{
			if( NULL != item.sDefault )
			{
				value = item.sDefault;
			} else {
				ret = -1;
				logerr( "ConfigRead: [%s]%s is not exist",
						item.sSection, item.sKey );
                continue;
			}
		}

		if( CONFIG_TYPE_STR == item.iType )
		{
			memset( item.pvAddr, 0, item.iSize );
			strncpy( (char*)item.pvAddr, value.c_str(), item.iSize );
		} else if( CONFIG_TYPE_INT == item.iType ) {
            if(item.iSize == sizeof(char)) {
                *(char*)item.pvAddr = (char)atoi( value.c_str() );
            } else if(item.iSize == sizeof(short)) {
                *(short*)item.pvAddr = (short)atoi( value.c_str() );
            } else if(item.iSize == sizeof(int)) {
                *(int*)item.pvAddr = atoi( value.c_str() );
            } else if(item.iSize == sizeof(long long)) {
                *(long long*)item.pvAddr = atoll( value.c_str() );
            }
		} else if( CONFIG_TYPE_UIN == item.iType ) {
            if(item.iSize == sizeof(unsigned char)) {
                *(unsigned char*)item.pvAddr = (unsigned char)strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned short)) {
                *(unsigned short*)item.pvAddr = (unsigned char)strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned int)) {
                *(unsigned int*)item.pvAddr = strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned long long)) {
                *(unsigned long long*)item.pvAddr = strtoull( value.c_str(), NULL, 10 );
            }
		} else if( CONFIG_TYPE_HEX == item.iType ) {
			*(unsigned int*)item.pvAddr = strtoul( value.c_str(), NULL, 16 );
		} else if( CONFIG_TYPE_STDSTR == item.iType ) {
			*(std::string*)item.pvAddr = value;
		} else {
			logerr( "ConfigRead: unknown type %d, [%s]%s",
					item.iType, item.sSection, item.sKey );
		}
	}

	return ret;
}


int ConfigRead( CConfig * config, ConfigItemInfoEx_t * infoArray, vector<string> * suffixList )
{
	int ret = 0;

	for( int i = 0; ; ++i )
	{
        ConfigItemInfoEx_t item = infoArray[i];

        if( NULL == item.pvAddr ) break;

        string value;

        vector<string>::iterator it = suffixList->begin();
        for( ;  it != suffixList->end(); it++) {

            value.clear();

            string & suffix = *it;

            char key[256];
            snprintf(key, sizeof(key), "%s%s", item.sKey, suffix.c_str());

            int readRet = 0;

            if( '\0' == item.sSection[0] )
            {
                readRet = config->ReadItem( key, "", value );
            } else {
                readRet = config->ReadItem( item.sSection, key, "", value );
            }

            if( 0 != readRet )
            {
                logerr( "ConfigRead: [%s]%s is not exist",
                        item.sSection, key );
                continue;
            } else {
                break;
            }
        }

        if(it == suffixList->end()) {
            if( NULL != item.sDefault  )
            {
                value = item.sDefault;
            } else {
                ret = -1;
                continue;
            }
        }

        if( CONFIG_TYPE_STR == item.iType )
        {
            memset( item.pvAddr, 0, item.iSize );
            strncpy( (char*)item.pvAddr, value.c_str(), item.iSize );
        } else if( CONFIG_TYPE_INT == item.iType ) {
            if(item.iSize == sizeof(char)) {
                *(char*)item.pvAddr = (char)atoi( value.c_str() );
            } else if(item.iSize == sizeof(short)) {
                *(short*)item.pvAddr = (short)atoi( value.c_str() );
            } else if(item.iSize == sizeof(int)) {
                *(int*)item.pvAddr = atoi( value.c_str() );
            } else if(item.iSize == sizeof(long long)) {
                *(long long*)item.pvAddr = atoll( value.c_str() );
            }
        } else if( CONFIG_TYPE_UIN == item.iType ) {
            if(item.iSize == sizeof(unsigned char)) {
                *(unsigned char*)item.pvAddr = (unsigned char)strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned short)) {
                *(unsigned short*)item.pvAddr = (unsigned char)strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned int)) {
                *(unsigned int*)item.pvAddr = strtoul( value.c_str(), NULL, 10 );
            } else if(item.iSize == sizeof(unsigned long long)) {
                *(unsigned long long*)item.pvAddr = strtoull( value.c_str(), NULL, 10 );
            }
        } else if( CONFIG_TYPE_HEX == item.iType ) {
            *(unsigned int*)item.pvAddr = strtoul( value.c_str(), NULL, 16 );
        } else if( CONFIG_TYPE_STDSTR == item.iType ) {
            *(std::string*)item.pvAddr = value;
        } else {
            logerr( "ConfigRead: unknown type %d, [%s]%s",
                    item.iType, item.sSection, item.sKey );
        }
    }

    return ret;
}


void ConfigDump( ConfigItemInfoEx_t * infoArray )
{
    map< string, vector<ConfigItemInfoEx_t> > dumpMap;

	for( int i = 0; ; ++i )
	{
		ConfigItemInfoEx_t item = infoArray[i];

		if( NULL == item.pvAddr ) break;

		dumpMap[ item.sSection ].push_back( item );
	}

	map< string, vector<ConfigItemInfoEx_t> >::iterator iter = dumpMap.begin();

	for( ; dumpMap.end() != iter; ++iter )
	{
		logerr("INIT: [%s]", iter->first.c_str() );

		vector<ConfigItemInfoEx_t> & list = iter->second;

		for( unsigned int i = 0; i < list.size(); ++i )
		{
			ConfigItemInfoEx_t & item = list[i];

			if( CONFIG_TYPE_STR == item.iType )
			{
				logerr("INIT: %s = %s",
						item.sKey, (char*)item.pvAddr );
			}

			if( CONFIG_TYPE_INT == item.iType )
			{
                if(item.iSize == sizeof(char)) {
                    logerr("INIT: %s = %d",
                            item.sKey, *(char*)item.pvAddr );
                } else if(item.iSize == sizeof(short)) {
                    logerr("INIT: %s = %d",
                            item.sKey, *(short*)item.pvAddr );
                } else if(item.iSize == sizeof(int)) {
                    logerr("INIT: %s = %d",
                            item.sKey, *(int*)item.pvAddr );
                } else if(item.iSize == sizeof(long long)) {
                    logerr("INIT: %s = %lld",
                            item.sKey, *(long long*)item.pvAddr );
                }
			}

			if( CONFIG_TYPE_UIN == item.iType )
			{
                if(item.iSize == sizeof(unsigned char)) {
                    logerr("INIT: %s = %u",
                            item.sKey, *(unsigned char*)item.pvAddr );
                } else if(item.iSize == sizeof(unsigned short)) {
                    logerr("INIT: %s = %u",
                            item.sKey, *(unsigned short*)item.pvAddr );
                } else if(item.iSize == sizeof(unsigned int)) {
                    logerr("INIT: %s = %u",
                            item.sKey, *(unsigned int*)item.pvAddr );
                } else if(item.iSize == sizeof(unsigned long long)) {
                    logerr("INIT: %s = %llu",
                            item.sKey, *(unsigned long long*)item.pvAddr );
                }
            }

            if( CONFIG_TYPE_STDSTR == item.iType ) {
                logerr("INIT: %s = %s",
                        item.sKey, ((std::string*)item.pvAddr)->c_str() );
        }
    }
    }
}




/* CConfig */
CConfig :: CConfig()
{
	m_pImpl = new CConfigImpl();
}

CConfig::CConfig( const std::string &configfile):
	m_pImpl(new CConfigImpl(configfile))
{
	this->LogReadConfigFile( configfile );
}


CConfig & CConfig::operator= (const CConfig& file)
{
	*m_pImpl = *file.m_pImpl;	
	return *this;
}

const string & CConfig :: getConfigFile()
{
	return m_pImpl->getConfigFile();
}

void CConfig :: SetConfigFile(const string &configfile)
{
	this->LogReadConfigFile( configfile );
	m_pImpl->SetConfigFile( configfile );
}

void CConfig :: LogReadConfigFile(const string& configfile) 
{
	if (g_setConfig == NULL) {
		g_setConfig = new set<string>;
	}

	set<string>& configSet = *(set<string>*)g_setConfig;
	if( configSet.end() == configSet.find( configfile ) )
	{
		configSet.insert( configfile );
	}
}

int CConfig::Init(void)
{
	return m_pImpl->Init();
}

int CConfig::Init(const string& sText)
{
	return m_pImpl->Init(sText);
}


CConfig::~CConfig(void)
{
	if ( NULL != m_pImpl )
	{
		delete m_pImpl;
	}
}


std::string CConfig::GetConfigContent(void)
{
	return m_pImpl->GetConfigContent();
}

int CConfig::getSection( const std::string& name,
		std::map<std::string,std::string>& section )
{
	return m_pImpl->getSection( name, section );
}

int CConfig::getSection( const std::string& name,
		std::vector<std::string> &section)
{
	return m_pImpl->getSection( name, section );
}

int CConfig :: getSectionText( const std::string& name,
		std::string & sectionText )
{
	return m_pImpl->getSectionText( name, sectionText );	
}

int CConfig::TrimString ( string & str )
{
	return CConfigImpl::TrimString( str );	
}

int CConfig :: TrimCStr( char * asString )
{
	return CConfigImpl::TrimCStr( asString );
}

void CConfig :: Split(const std::string& str, const std::string& delim,
		std::vector<std::string>& output)
{
	
	CConfigImpl :: Split( str, delim, output );
}

int CConfig::getSectionList( std::vector<std::string>& sectionlist )
{
	return m_pImpl->getSectionList( sectionlist );      
}

void CConfig::dumpinfo(void)
{
	m_pImpl->dumpinfo();
}

int CConfig :: AddItem( const std::string& section,
		const std::string& key, const std::string& value )
{
	return m_pImpl->AddItem( section, key, value );
}

int CConfig::ReadItem( const std::string& section,
		const std::string& key, const std::string& defaultvalue,
		std::string& itemvalue )
{
	
	return m_pImpl->ReadItem( section, key, defaultvalue, itemvalue );
}

int CConfig::ReadItem( const std::string& section,
		const std::string& key, const std::string& defaultvalue,
		std::string& itemvalue,
        vector<string> * suffixList)
{
	
	return m_pImpl->ReadItem( section, key, defaultvalue, itemvalue, suffixList );
}


int CConfig::ReadItem( const std::string& section, const std::string& key, int defaultvalue, int& itemvalue )
{
    string sValue;
    string sDefault = std::to_string(defaultvalue);
    int ret = m_pImpl->ReadItem(section, key, sDefault, sValue);
    itemvalue = atoi(sValue.c_str());
    return ret;
}

int CConfig::ReadItem( const std::string& section, const std::string& key,
        int defaultvalue, int& itemvalue,
        vector<string> * suffixList )
{
    string sValue;
    string sDefault = std::to_string(defaultvalue);
    int ret = m_pImpl->ReadItem(section, key, sDefault, sValue, suffixList);
    itemvalue = atoi(sValue.c_str());
    return ret;
}


int CConfig::ReadItem( const std::string& section, const std::string& key, 
		unsigned int defaultvalue, unsigned int & itemvalue )
{
    string sValue;
    string sDefault = std::to_string(defaultvalue);
    int ret = m_pImpl->ReadItem(section, key, sDefault, sValue);
    itemvalue = (unsigned int)strtoul(sValue.c_str(), NULL, 10);
    return ret;
}

int CConfig::ReadItem( const std::string& section, const std::string& key, 
        unsigned int defaultvalue, unsigned int & itemvalue,
        vector<string> * suffixList )
{
    string sValue;
    string sDefault = std::to_string(defaultvalue);
    int ret = m_pImpl->ReadItem(section, key, sDefault, sValue, suffixList);
    itemvalue = (unsigned int)strtoul(sValue.c_str(), NULL, 10);
    return ret;
}

int CConfig::ReadRawItem( const std::string& section,
		const std::string& key, const std::string& defaultvalue,
		std::string& itemvalue )
{
	return m_pImpl->ReadRawItem( section, key, defaultvalue, itemvalue );	
}

int CConfig::ReadRawItem( const std::string& section, const std::string& key, int defaultvalue, int& itemvalue )
{
    string sValue;
    string sDefault = std::to_string(defaultvalue);
    int ret = m_pImpl->ReadRawItem(section, key, sDefault, sValue);
    itemvalue = atoi(sValue.c_str());
    return ret;
}

int CConfig :: ReadItem( const std::string& key, const std::string& defaultvalue, std::string& itemvalue )
{
	return ReadItem( CGIMAGIC_UN_SECTIONED, key, defaultvalue, itemvalue );
}

int CConfig::ReadItem(const std::string& key, int defaultvalue, int& itemvalue )
{
    return ReadItem(CGIMAGIC_UN_SECTIONED, key, defaultvalue, itemvalue);
}

int CConfig::ReadItem(const std::string& key, unsigned int defaultvalue, unsigned int & itemvalue )
{
    return ReadItem(CGIMAGIC_UN_SECTIONED, key, defaultvalue, itemvalue);
}

int CConfig :: ReadItem( const std::string& key, const std::string& defaultvalue,
        std::string& itemvalue, vector<string> * suffixList )
{
	return ReadItem( CGIMAGIC_UN_SECTIONED, key, defaultvalue, itemvalue, suffixList );
}

int CConfig::ReadItem(const std::string& key, int defaultvalue, int& itemvalue, vector<string> * suffixList )
{
    return ReadItem(CGIMAGIC_UN_SECTIONED, key, defaultvalue, itemvalue, suffixList);
}

int CConfig::ReadItem(const std::string& key, unsigned int defaultvalue, unsigned int & itemvalue, vector<string> * suffixList )
{
    return ReadItem(CGIMAGIC_UN_SECTIONED, key, defaultvalue, itemvalue, suffixList);
}


}

