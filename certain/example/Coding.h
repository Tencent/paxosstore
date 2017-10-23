#pragma once

#include <stdint.h>

#include "DBType.h"
#include "utils/Time.h"

extern const int kEntityMetaKeyLen;
extern const int kInfoKeyLen;
extern const int kPLogKeyLen;
extern const int kPLogValueKeyLen;
extern const int kPLogMetaKeyLen;

uint64_t htonll(uint64_t i);
uint64_t ntohll(uint64_t i);

void AppendFixed8(std::string& key, uint8_t i);
void AppendFixed16(std::string& key, uint16_t i);
void AppendFixed32(std::string& key, uint32_t i);
void AppendFixed64(std::string& key, uint64_t i);

void RemoveFixed8(dbtype::Slice& key, uint8_t& i);
void RemoveFixed16(dbtype::Slice& key, uint16_t& i);
void RemoveFixed32(dbtype::Slice& key, uint32_t& i);
void RemoveFixed64(dbtype::Slice& key, uint64_t& i);

void EncodeEntityMetaKey(std::string& strKey, uint64_t iEntityID);
bool DecodeEntityMetaKey(dbtype::Slice strKey, uint64_t& iEntityID);

void EncodeInfoKey(std::string& strKey, uint64_t iEntityID, uint64_t iInfoID);
bool DecodeInfoKey(dbtype::Slice strKey, uint64_t& iEntityID, uint64_t& iInfoID);

bool KeyHitEntityID(uint64_t iEntityID, const dbtype::Slice& strKey);

void EncodePLogKey(std::string& strKey, uint64_t iEntityID, uint64_t iEntry);
bool DecodePLogKey(dbtype::Slice strKey, uint64_t& iEntityID, uint64_t& iEntry,
        uint64_t *piTimestampMS = NULL);

void EncodePLogValueKey(std::string& strKey, uint64_t iEntityID,
        uint64_t iEntry, uint64_t iValueID);
bool DecodePLogValueKey(dbtype::Slice strKey, uint64_t& iEntityID,
        uint64_t& iEntry, uint64_t& iValueID, uint64_t *piTimestampMS = NULL);

void EncodePLogMetaKey(std::string& strKey, uint64_t iEntityID);
bool DecodePLogMetaKey(dbtype::Slice strKey, uint64_t& iEntityID);

uint64_t GetEntityID(uint64_t iKey, uint64_t iEntityNum = 200);
