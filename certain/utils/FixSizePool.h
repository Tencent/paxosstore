#ifndef CERTAIN_UTILS_FIXSIZEPOLL_H_
#define CERTAIN_UTILS_FIXSIZEPOLL_H_

#include "utils/Logger.h"

namespace Certain
{

class clsFixSizePool
{
public:
    clsFixSizePool(int iItemCnt, int iItemSize);
    ~clsFixSizePool();

    char *Alloc(int iItemSize);
    void Free(char * pItem);

private:
    int m_iItemCnt;
    int m_iItemSize;
    char *m_pItem;

    volatile int m_iHead;
    volatile int m_iAllocCnt;

    int *GetNext(int iIndex)
    {
        return (int*)(m_pItem + iIndex * uint64_t(m_iItemSize));
    }
};

} // namespace Certain

#endif
