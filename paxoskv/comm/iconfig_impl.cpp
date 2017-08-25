
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#define COMM2_LOCAL_USE
#include "iconfig_impl.h"
#include "cutils/log_utils.h"

using namespace std;

#define CGIMAGIC_UN_SECTIONED "CGIMAGIC-UN-SECTIONED"
#define CGIMAGIC_SECTION_TEXT "CGIMAGIC-SECTION-TEXT"

#define CGIMAGIC_SECTION_BUFFER "CGIMAGIC-SECTION-BUFFER"
#define CGIMAGIC_KEY_BUFFER     "CGIMAGIC-KEY-BUFFER"

#define MAX_LINE_LEN  1024*32
namespace Comm {
/* ConfigImpl */
CConfigImpl :: CConfigImpl() 
{
	m_bIsInited = false;
}

CConfigImpl :: CConfigImpl(const std::string& configfile)
{
	m_config = configfile; 
	m_bIsInited = false;
}

CConfigImpl & CConfigImpl :: operator= (const CConfigImpl& file)
{
	this->m_config = file.m_config;
	this->m_config_buf = file.m_config_buf;
	this->Init();
	return *this;
}

const string & CConfigImpl :: getConfigFile()
{
	return m_config;
}

void CConfigImpl :: SetConfigFile(const string &configfile)
{
	m_config = configfile;
	m_bIsInited = false;
}

int CConfigImpl::Init(void)
{
	int ret = LoadFile();
	if ( 0 == ret ) {
		m_bIsInited = true;
	}
	return ret;
}

int CConfigImpl::Init(const string& text)
{
	int ret = LoadText(text);
	if ( 0 == ret ) {
		m_bIsInited = true;
	}
	return ret;	
}


CConfigImpl::~CConfigImpl()
{
}

void CConfigImpl :: StrToLower(string &str)
{
	int size = str.size();
	for ( int i=0; i < size; i++ ) str[i] = tolower(str[i]);
}

int CConfigImpl :: ParserConfig( const char * src, int filelen )
{
	CStringSlice section, key, value;

	m_hash.Clear();
	m_sectionList.clear();

	const char *eol, *tmp;	

	for ( const char *ptr = src; filelen > 0;  )
	{
		eol = strchr( ptr, '\n' ); // for each line 

		if ( eol == NULL ) eol = ptr + filelen; // last line

		filelen -= ( eol - ptr +1);

		if ( ptr < eol )
		{
			while( ptr < eol && ( *ptr == ' ' || *ptr == '\t' )) ptr++; // ltrim
			if ( *ptr == '[' ) { //section
				ptr += 1;
				tmp = ptr;
				for ( ; *ptr != ']'  && *ptr != '\n' && ptr < eol; ptr++) ;
				
				section = CStringSlice(tmp, ptr);	
				section.StrTrim("\t ");
				
				//StrToLower(section);

				m_sectionList.push_back(section);	

			}  else if ( NULL == strchr( "#;\n", *ptr) ) { //item
				tmp = ptr;
				for ( ; *ptr != '=' && ptr < eol; ptr++ ) ;
				
				key = CStringSlice(tmp, ptr);
				key.StrTrim("\t ");
				//StrToLower(key);

				if ( *ptr == '=' && section.GetStart() != NULL) {
					ptr += 1;

					tmp = ptr;
					for ( ; *ptr != '\n' && *ptr != '\r' && ptr < eol ; ptr++ ) ; 

					value = CStringSlice(tmp, ptr);
					value.StrTrim("\t ");

					m_hash.Add(section, key, value);
				} 
			}
		}

		ptr = eol + 1;
	}
	
	return m_sectionList.size() > 0 ? 0 : -1;
	
}

int CConfigImpl::LoadFile(void)
{	
	FILE * fp = fopen( m_config.c_str(), "r" );

	m_sectionList.clear();

	if( NULL != fp )
	{
		struct stat fileStat;
		if( 0 == fstat( fileno( fp ), &fileStat ) )
		{
			//logerr(  "CConfigImpl LoadFile(%s) OK.", m_config.c_str() );
					
			char * tmp = (char*)malloc( fileStat.st_size + 64 );

			// TRICK: add CGIMAGIC_UN_SECTIONED at the begin
			int padLen = snprintf( tmp, 64, "\n[%s]\n", CGIMAGIC_UN_SECTIONED );

			fread( tmp + padLen, fileStat.st_size, 1, fp );
			tmp[ fileStat.st_size + padLen ] = '\0';

			m_config_buf = tmp;
			
			// parser configfile to hash
			ParserConfig( m_config_buf.c_str() , m_config_buf.size());

			free( tmp );
		} else {
			logerr(  "MAGIC_CONFIG fstat(%s) fail, errno %d, %s",
					m_config.c_str(), errno, strerror( errno ) );
		}

		fclose( fp ), fp = NULL;
	} else {
		//ErrLog( "MAGIC_CONFIG fopen(%s) fail, errno %d, %s",
				//m_config.c_str(), errno, strerror( errno ) );
	}

	return m_sectionList.size() == 0 ? -1 : 0;
}

int CConfigImpl::LoadText(const string& text)
{

	m_sectionList.clear();


	char * tmp = (char*)malloc( text.size() + 64 );

	// TRICK: add CGIMAGIC_UN_SECTIONED at the begin
	int padLen = snprintf( tmp, 64, "\n[%s]\n", CGIMAGIC_UN_SECTIONED );

	memcpy( tmp + padLen, text.data(), text.size() );
	tmp[ text.size() + padLen ] = '\0';

	m_config_buf = tmp;
			
	// parser configfile to hash
	ParserConfig( m_config_buf.c_str() , m_config_buf.size());

	free( tmp ); 

	return m_sectionList.size() == 0 ? -1 : 0;
}


const char * CConfigImpl :: GetBuffer()
{
	return m_config_buf.c_str();
}

std::string CConfigImpl::GetConfigContent(void)
{
	return GetBuffer();
}

int CConfigImpl::getSection( const std::string& name,
		std::map<std::string,std::string>& section )
{
	if ( ! m_bIsInited ) Init();

	const char * src = GetBuffer();

	char tmpSection[ 128 ] = { 0 };
	snprintf( tmpSection, sizeof( tmpSection ), "\n[%s]", name.c_str() );

    int line_len = 4096;
    char * line  = new char[line_len];
    if(!line) {
        return -1;
    }
    memset(line, 0x0, line_len);

	const char * pos = strstr( src, tmpSection );
	if( NULL != pos ) pos++;

	for( ; NULL != pos; )
	{
		pos = strchr( pos, '\n' );

		if( NULL == pos ) break;
		pos++;

		if( '[' == *pos ) break;

		if( ';' == *pos || '#' == *pos ) continue;

		strncpy( line, pos, line_len - 1 );

		char * tmpPos = strchr( line, '\n' );
		if( NULL != tmpPos ) *tmpPos = '\0';

		char * value = strchr( line, '=' );
		if( NULL != value )
		{
			*value = '\0';
			value++;

			TrimCStr( line );
			TrimCStr( value );

			if( '\0' != line[0] ) section[ line ] = value;
		}
	}

    delete [] line;
	return NULL != strstr( src, tmpSection ) ? 0 : -1;
	
}

int CConfigImpl :: getSection( const std::string& name,
		std::vector<std::string> &section )
{
	if ( ! m_bIsInited ) Init();

	const char * src = GetBuffer();

	char tmpSection[ 128 ] = { 0 };
	snprintf( tmpSection, sizeof( tmpSection ), "\n[%s]", name.c_str() );

    int line_len = 4096;
    char * line  = new char[line_len];
    if(!line) {
        return -1;
    }
    memset(line, 0x0, line_len);

	const char * pos = strstr( src, tmpSection );
	if( NULL != pos ) pos++;

	for( ; NULL != pos; )
	{
		pos = strchr( pos, '\n' );

		if( NULL == pos ) break;
		pos++;

		if( '[' == *pos ) break;

		if( ';' == *pos || '#' == *pos ) continue;

		strncpy( line, pos, line_len - 1 );

		char * tmpPos = strchr( line, '\n' );
		if( NULL != tmpPos ) *tmpPos = '\0';

		TrimCStr( line );

		if( '\0' != line[0] ) section.push_back( line );
	}

    delete [] line;
	return NULL != strstr( src, tmpSection ) ? 0 : -1;
}

int CConfigImpl :: getSectionText( const std::string& name,
		std::string & sectionText )
{
	if ( ! m_bIsInited ) Init();

	const char * src = GetBuffer();

	char tmpSection[ 128 ] = { 0 };
	snprintf( tmpSection, sizeof( tmpSection ), "\n[%s]", name.c_str() );

	const char * pos = strstr( src, tmpSection );
	if( NULL != pos )
	{
		pos = strchr( pos + 1, '\n' );
		if( NULL != pos )
		{
			const char * endPos = strstr( pos, "\n[" );
			if( NULL == endPos ) endPos = strchr( pos, '\0' );

			if( endPos > pos ) sectionText.append( pos + 1, endPos - pos - 1 );
		}
	}

	return NULL != pos ? 0 : -1;
}


int CConfigImpl::getSectionList( std::vector<std::string>& sectionlist )
{
	if ( ! m_bIsInited ) Init();

	for ( size_t i=0; i<m_sectionList.size(); i++ )			
	{
		sectionlist.push_back(string(m_sectionList[i].GetStart(),
					m_sectionList[i].GetEnd() - m_sectionList[i].GetStart()));
	}
	return 0;       
}

void CConfigImpl::dumpinfo(void)
{
	vector<string> sectionList;

	getSectionList( sectionList );

	for( vector<string>::iterator iter = sectionList.begin(); sectionList.end() != iter; ++iter )
	{
		map< string, string > keyValMap;

		cout << "[" << *iter << "]" << endl;

		getSection( *iter, keyValMap );

		for( map< string, string >::iterator mit = keyValMap.begin(); keyValMap.end() != mit; ++mit )
		{
			cout << mit->first << " = " << mit->second << endl;
		}
	}

}

int CConfigImpl :: AddItem( const std::string& section,
		const std::string& key, const std::string& value )
{

	int size = 0;

	const char * src = GetBuffer();
	const char * pos = GetItemPos( src, section.c_str(), key.c_str(), &size );

	string newBuffer;

	if( NULL != pos )
	{
		newBuffer.append( src, pos - src );
		newBuffer.append( key ).append( " = " ).append( value );
		if( size <= 0 ) newBuffer.append( "\n" );
		newBuffer.append( pos + size );
	} else {
		newBuffer.append( src );
		newBuffer.append( "\n[" ).append( section ).append( "]\n" );
		newBuffer.append( key ).append( " = " ).append( value ).append( "\n" );
	}
	
	m_config_buf = newBuffer;	
	
	// re parser config
	
	ParserConfig( m_config_buf.c_str(), m_config_buf.size() );

	return 0;
}

int CConfigImpl::ReadItem( const std::string& section,
		const std::string& key, const std::string& defaultvalue,
		std::string& itemvalue )
{
	if( section.size() <= 0 || key.size() <= 0 ) return -1;

	if ( ! m_bIsInited ) Init();

	string tmpsec,tmpkey;
	//tmpsec = section; StrToLower(tmpsec);
	//tmpkey = key; StrToLower(tmpkey);
	
	CStringSlice idxSec = section;
	CStringSlice idxKey = key;

	CStringSlice val = m_hash.Get(idxSec, idxKey);
	if ( val.GetStart() != NULL )	 
	{
		itemvalue.assign(val.GetStart(), val.GetEnd() - val.GetStart());
		return 0;
	}

	// not exist, use default value
	// return -1
	itemvalue = defaultvalue;
	return -1;

}

int CConfigImpl::ReadItem( const std::string& section,
		const std::string& key, const std::string& defaultvalue,
		std::string& itemvalue,
        vector<string> * suffixList )
{
    if(!suffixList) {
        return -1;
    }
    int ret = 0;
    vector<string>::iterator it = suffixList->begin();
    for( ;  it != suffixList->end(); it++) {

        string & suffix = *it;

        string tmp_key;
        tmp_key.append(key);
        tmp_key.append(suffix);

        string value;

        ret = ReadItem(section, tmp_key, defaultvalue,
                value);

        if( 0 != ret )
        {
            logerr( "ConfigRead: [%s]%s is not exist",
                    section.c_str(), tmp_key.c_str() );
            continue;
        } else {
            itemvalue = value;
        }
        break;
    }
    if(it == suffixList->end()) {
        itemvalue = defaultvalue;
    }
    return ret;
}

int CConfigImpl::ReadRawItem( const std::string& section,
		const std::string& key, const std::string& defaultvalue,
		std::string& itemvalue )
{
	if( section.size() <= 0 || key.size() <= 0 ) return -1;

	if ( ! m_bIsInited ) Init();

	int size = 0;

	const char * src = GetBuffer();
	const char * pos = GetItemPos( src, section.c_str(), key.c_str(), &size );

	if( NULL != pos && size > 0 )
	{
		const char * tmpPos = strchr( pos, '=' );

		if( NULL != tmpPos && ( tmpPos - pos ) < size )
		{
			itemvalue = "";
			itemvalue.append( tmpPos + 1, pos + size - tmpPos - 1 );
			TrimString( itemvalue );
		}
	} else {
		itemvalue = defaultvalue;
	}

	return ( NULL != pos && size > 0 ) ? 0 : -1;
}

int CConfigImpl::TrimString ( string & str )
{
	string drop = " \r\n";
	// Trim right
	str  = str.erase( str.find_last_not_of(drop)+1 );
	// Trim left
	str.erase( 0, str.find_first_not_of(drop) );

	return 0;
}

int CConfigImpl :: TrimCStr( char * asString )
{
	register int liLen ;
	register char *lp ;

	liLen = strlen ( asString ) ;
	while ( liLen > 0 && isspace( asString [liLen - 1] ) )
		liLen -- ;
	asString [liLen] = '\0' ;
	for ( lp = asString; isspace(*lp); lp ++ ) ;
	if ( lp != asString )
		memmove ( asString, lp, liLen + 1 ) ;

	return 0;
}

void CConfigImpl :: Split(const std::string& str, const std::string& delim,
		std::vector<std::string>& output)
{
	
	unsigned int offset = 0;

#if defined (__LP64__) || defined (__64BIT__) || defined (_LP64) || (__WORDSIZE == 64)
	unsigned long long  delimIndex = 0;
#else
	unsigned int delimIndex = 0;
#endif

	delimIndex = str.find(delim, offset);

	while (delimIndex != string::npos)
	{
		output.push_back(str.substr(offset, delimIndex - offset));
		offset += delimIndex - offset + delim.length();
		delimIndex = str.find(delim, offset);
	}

	output.push_back(str.substr(offset));
}

const char * CConfigImpl :: GetItemPos( const char * src,
		const char * section, const char * key, int * size )
{
	const char * ret = NULL;

	*size = 0;

	char tmpSection[ 128 ] = { 0 };
	snprintf( tmpSection, sizeof( tmpSection ), "\n[%s]", section );

	char tmpKey[ 128 ] = { 0 };
	snprintf( tmpKey, sizeof( tmpKey ), "\n%s", key );

	const char * endPos = NULL;
	const char * pos = strcasestr( src, tmpSection );
	if( NULL != pos )
	{
		pos = strchr( pos + 1, '\n' );
		if( NULL == pos ) pos = strchr( src, '\0' );

		endPos = strstr( pos, "\n[" );
		if( NULL == endPos ) endPos = strchr( pos, '\0' );
	}

	for( ; NULL != pos && pos < endPos; )
	{
		pos = strcasestr( pos, tmpKey );

		if( NULL == pos || pos > endPos ) break;

		const char * tmpPos = pos + strlen( tmpKey );
		if( ( !isspace( *tmpPos ) ) && ( '=' != *tmpPos ) ) continue;

		pos++;

		const char * eol = strchr( pos, '\n' );
		if( NULL == eol ) eol = strchr( pos, '\0' );

		tmpPos = strchr( pos, '=' );
		if( NULL != tmpPos && tmpPos < eol )
		{
			ret = pos;
			*size = eol - pos;

			break;
		}
	}

	if( NULL == ret )
	{
		ret = endPos;
		*size = 0;
	}

	return ret;
}
}

