#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "example/DBType.h"

enum KVStatus
{
    VALUE  = 1,
    DELETE = 2,
};

typedef std::pair<KVStatus, std::string> InfoValue;

class clsTemporaryTable
{
public:
    clsTemporaryTable(dbtype::DB *poLevelDB);
    int Put(const std::string &strKey, const std::string &strValue);
    int Get(const std::string &strKey, std::string &strValue, const dbtype::Snapshot* poSnapshot = NULL);
    int Delete(const std::string& strKey);
    const std::string& GetWriteBatchString();

private:
    // InfoKey -> InfoValue <VALUE/DELETE, proto_string>
    std::map<std::string, InfoValue> m_oKVMap;
    dbtype::WriteBatch m_oWriteBatch;
    dbtype::DB *m_poLevelDB;
};
