#pragma once

#include <stdint.h>

#include "utils/Time.h"

#include "DBType.h"
#include "slice.h"

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
void AppendBuffer(std::string& key, const char* buffer, uint32_t size);
void AppendString(std::string& key, const std::string& str, uint32_t size);

void RemoveFixed8(dbtype::Slice& key, uint8_t& i);
void RemoveFixed16(dbtype::Slice& key, uint16_t& i);
void RemoveFixed32(dbtype::Slice& key, uint32_t& i);
void RemoveFixed64(dbtype::Slice& key, uint64_t& i);
void RemoveBuffer(dbtype::Slice& key, char* buffer, uint32_t size);
void RemoveString(dbtype::Slice& key, std::string& str, uint32_t size);

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
