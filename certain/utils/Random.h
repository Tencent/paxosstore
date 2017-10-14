#ifndef CERTAIN_UTILS_RANDOM_H_
#define CERTAIN_UTILS_RANDOM_H_

#include <stdint.h>

namespace Certain {

class clsRandom
{
private:
    uint32_t m_iSeed;

public:
    explicit clsRandom(uint32_t s) : m_iSeed(s & 0x7fffffffu)
    {
        // Avoid bad seeds.
        if (m_iSeed == 0 || m_iSeed == 2147483647L)
        {
            m_iSeed = 1;
        }
    }

    uint32_t Next()
    {
        // The Random algorith from leveldb for 32bit.
        static const uint32_t M = 2147483647L;
        static const uint64_t A = 16807;

        uint64_t iProduct = m_iSeed * A;
        m_iSeed = static_cast<uint32_t>((iProduct >> 31) + (iProduct & M));

        if (m_iSeed > M)
        {
            m_iSeed -= M;
        }

        return m_iSeed;
    }
};

}  // namespace Certain

#endif  // CERTAIN_UTILS_RANDOM_H_
