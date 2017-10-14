#ifndef CERTAIN_UTILS_HASH_
#define CERTAIN_UTILS_HASH_

namespace Certain
{

// Murmur hash impl.
inline uint32_t Hash(const char* pcData, int iLen)
{
    uint32_t  h, k;

    h = 0 ^ iLen;

    while (iLen >= 4)
    {
        k  = pcData[0];
        k |= pcData[1] << 8;
        k |= pcData[2] << 16;
        k |= pcData[3] << 24;

        k *= 0x5bd1e995;
        k ^= k >> 24;
        k *= 0x5bd1e995;

        h *= 0x5bd1e995;
        h ^= k;

        pcData += 4;
        iLen -= 4;
    }

    switch (iLen)
    {
        case 3:
            h ^= pcData[2] << 16;
        case 2:
            h ^= pcData[1] << 8;
        case 1:
            h ^= pcData[0];
            h *= 0x5bd1e995;
    }

    h ^= h >> 13;
    h *= 0x5bd1e995;
    h ^= h >> 15;

    return h;
}

inline uint32_t Hash(const string &strData)
{
    return Hash(strData.c_str(), strData.size());
}

inline uint32_t Hash(uint64_t iData)
{
    return Hash((const char *)&iData, sizeof(iData));
}

} // namespace Certain

#endif // CERTAIN_UTILS_HASH_
