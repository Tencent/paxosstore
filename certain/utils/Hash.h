#ifndef CERTAIN_UTILS_HASH_
#define CERTAIN_UTILS_HASH_

namespace leveldb
{
extern uint32_t Hash(const char* data, size_t n, uint32_t seed);
}

namespace Certain
{

inline uint32_t Hash(const char* pcData, uint32_t iLen)
{
	return leveldb::Hash(pcData, iLen, 20151208);
}

inline uint32_t Hash(const string &strData)
{
	return Hash(strData.c_str(), strData.size());
}

inline uint32_t Hash(uint64_t iData)
{
	return Hash((const char *)&iData, sizeof(iData));
}

inline uint32_t dictIntHashFunctionKvsvr(uint32_t key)
{
	key += ~(key << 15); 
	key ^=  (key >> 10); 
	key +=  (key << 3);
	key ^=  (key >> 6);
	key += ~(key << 11); 
	key ^=  (key >> 16); 
	return key;
}

} // namespace Certain

#endif // CERTAIN_UTIL_HASH_
