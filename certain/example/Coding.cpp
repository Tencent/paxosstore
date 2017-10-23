#include "Coding.h"

#include <arpa/inet.h>

const int kEntityMetaKeyLen = 10;
const int kInfoKeyLen = 19;
const int kPLogKeyLen = (sizeof(uint64_t) * 3 + 1);
const int kPLogValueKeyLen = (sizeof(uint64_t) * 4 + 1);
const int kPLogMetaKeyLen = (sizeof(uint64_t) * 2 + 1);

uint64_t ntohll(uint64_t i)
{
	if (__BYTE_ORDER == __LITTLE_ENDIAN) {
		return (((uint64_t)ntohl((uint32_t)i)) << 32) | (uint64_t)(ntohl((uint32_t)(i >> 32)));
	}
    return i;
}

uint64_t htonll(uint64_t i)
{
	if (__BYTE_ORDER == __LITTLE_ENDIAN) {
		return (((uint64_t)htonl((uint32_t)i)) << 32) | ((uint64_t)htonl((uint32_t)(i >> 32)));
	}
    return i;
}

void AppendFixed8(std::string& key, uint8_t i)
{
    key.push_back(i);
}

void AppendFixed16(std::string& key, uint16_t i)
{
    i = htons(i);
    key.append((char *)&i, sizeof(i));
}

void AppendFixed32(std::string& key, uint32_t i)
{
    i = htonl(i);
    key.append((char *)&i, sizeof(i));
}

void AppendFixed64(std::string& key, uint64_t i)
{
    i = htonll(i);
    key.append((char *)&i, sizeof(i));
}

void RemoveFixed8(dbtype::Slice& key, uint8_t& i)
{
    i = *(uint8_t *)key.data();
    key.remove_prefix(sizeof(i));
}

void RemoveFixed16(dbtype::Slice& key, uint16_t& i)
{
    i = ntohs(*(uint16_t *)key.data());
    key.remove_prefix(sizeof(i));
}

void RemoveFixed32(dbtype::Slice& key, uint32_t& i)
{
    i = ntohl(*(uint32_t *)key.data());
    key.remove_prefix(sizeof(i));
}

void RemoveFixed64(dbtype::Slice& key, uint64_t& i)
{
    i = ntohll(*(uint64_t *)key.data());
    key.remove_prefix(sizeof(i));
}

void EncodeEntityMetaKey(std::string& strKey, uint64_t iEntityID)
{
    strKey.reserve(kEntityMetaKeyLen);
    AppendFixed8(strKey, 0);
    AppendFixed64(strKey, iEntityID);
    AppendFixed8(strKey, 1);
}

bool DecodeEntityMetaKey(dbtype::Slice strKey, uint64_t& iEntityID)
{
    if (strKey.size() != kEntityMetaKeyLen) return false;
    uint8_t cZero, cOne;

    RemoveFixed8(strKey, cZero);
    RemoveFixed64(strKey, iEntityID);
    RemoveFixed8(strKey, cOne);

    return (cZero == 0 && cOne == 1);
}

void EncodeInfoKey(std::string& strKey, uint64_t iEntityID, uint64_t iInfoID)
{
    strKey.reserve(kInfoKeyLen);
    AppendFixed8(strKey, 1);
    AppendFixed64(strKey, iEntityID);
    AppendFixed8(strKey, 0);
    AppendFixed64(strKey, iInfoID);
    AppendFixed8(strKey, 2);
}

bool DecodeInfoKey(dbtype::Slice strKey, uint64_t& iEntityID, uint64_t& iInfoID)
{
    if (strKey.size() != kInfoKeyLen) return false;
    uint8_t cZero, cOne, cTwo;

    RemoveFixed8(strKey, cOne);
    RemoveFixed64(strKey, iEntityID);
    RemoveFixed8(strKey, cZero);
    if (cOne != 1 || cZero != 0) return false;

    RemoveFixed64(strKey, iInfoID);
    RemoveFixed8(strKey, cTwo);
    return cTwo == 2;
}

bool KeyHitEntityID(uint64_t iEntityID, const dbtype::Slice& strKey)
{
    uint64_t iRealEntityID, iRealInfoID;
    return (DecodeInfoKey(strKey, iRealEntityID, iRealInfoID) && 
            (iRealEntityID == iEntityID));
}

void EncodePLogKey(std::string& strKey, uint64_t iEntityID, uint64_t iEntry)
{
    uint8_t cZero = 0;
    assert(iEntry > 0);
    strKey.reserve(kPLogKeyLen);
    AppendFixed64(strKey, Certain::GetCurrTimeMS());
    AppendFixed8(strKey, cZero);
    AppendFixed64(strKey, iEntityID);
    AppendFixed64(strKey, iEntry);
}

bool DecodePLogKey(dbtype::Slice strKey, uint64_t& iEntityID, uint64_t& iEntry,
        uint64_t *piTimestampMS)
{
    uint64_t iTimestampMS;
    uint8_t cZero;
    if (strKey.size() != kPLogKeyLen) return false;

    RemoveFixed64(strKey, iTimestampMS);
    if (piTimestampMS != NULL)
    {
        *piTimestampMS = iTimestampMS;
    }

    RemoveFixed8(strKey, cZero);
    RemoveFixed64(strKey, iEntityID);
    RemoveFixed64(strKey, iEntry);

    return cZero == 0 && iEntry > 0;
}

void EncodePLogValueKey(std::string& strKey, uint64_t iEntityID,
        uint64_t iEntry, uint64_t iValueID)
{
    uint8_t cZero = 0;
    assert(iEntry > 0);
    assert(iValueID > 0);
    strKey.reserve(kPLogValueKeyLen);
    AppendFixed64(strKey, Certain::GetCurrTimeMS());
    AppendFixed8(strKey, cZero);
    AppendFixed64(strKey, iEntityID);
    AppendFixed64(strKey, iEntry);
    AppendFixed64(strKey, iValueID);
}

bool DecodePLogValueKey(dbtype::Slice strKey, uint64_t& iEntityID,
        uint64_t& iEntry, uint64_t& iValueID, uint64_t *piTimestampMS)
{
    uint64_t iTimestampMS;

    uint8_t cZero;
    if (strKey.size() != kPLogValueKeyLen) return false;

    RemoveFixed64(strKey, iTimestampMS);
    if (piTimestampMS != NULL)
    {
        *piTimestampMS = iTimestampMS;
    }

    RemoveFixed8(strKey, cZero);
    RemoveFixed64(strKey, iEntityID);
    RemoveFixed64(strKey, iEntry);
    RemoveFixed64(strKey, iValueID);

    return cZero == 0 && iEntry > 0 && iValueID > 0;
}

void EncodePLogMetaKey(std::string& strKey, uint64_t iEntityID)
{
    uint8_t cOne = 1;
    strKey.reserve(kPLogMetaKeyLen);
    AppendFixed64(strKey, 0);
    AppendFixed8(strKey, cOne);
    AppendFixed64(strKey, iEntityID);
}

bool DecodePLogMetaKey(dbtype::Slice strKey, uint64_t& iEntityID)
{
    uint64_t iTimestampMS; // zero placeholder
    uint8_t cOne;
    if (strKey.size() != kPLogMetaKeyLen) return false;

    RemoveFixed64(strKey, iTimestampMS);
    RemoveFixed8(strKey, cOne);
    RemoveFixed64(strKey, iEntityID);

    return cOne == 1 && iTimestampMS == 0;
}

uint64_t GetEntityID(uint64_t iKey, uint64_t iEntityNum)
{
    return iKey % iEntityNum;
}
