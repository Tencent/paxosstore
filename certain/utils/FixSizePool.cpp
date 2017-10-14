#include "utils/FixSizePool.h"

namespace Certain
{

clsFixSizePool::clsFixSizePool(int iItemCnt, int iItemSize)
{
    assert(iItemSize > int(sizeof(int)));
    assert(iItemCnt > 1);

    m_iItemSize = iItemSize;
    m_iItemCnt = iItemCnt;
    m_pItem = (char*)calloc(iItemCnt, iItemSize);
    assert(m_pItem != NULL);

    for(int i = 0; i < iItemCnt - 1; i++)
    {
        *GetNext(i) = i + 1;
    }

    *GetNext(iItemCnt - 1) = -1;
    m_iHead = 0;
    m_iAllocCnt = 0;
}

clsFixSizePool::~clsFixSizePool()
{
    free(m_pItem);
}

char *clsFixSizePool::Alloc(int iItemSize)
{
    char *tmp = NULL;
    if (iItemSize <= m_iItemSize && m_iHead != -1)
    {
        assert(-1 <= m_iHead && m_iHead < m_iItemCnt);

        if(m_iHead != -1)
        {
            tmp = m_pItem + m_iHead * uint64_t(m_iItemSize);
            m_iHead = *GetNext(m_iHead);
            m_iAllocCnt++;
        }
        else
        {
            tmp = (char*)malloc(iItemSize);
        }
    }
    else
    {
        tmp = (char*)malloc(iItemSize);
    }

    return tmp;
}

void clsFixSizePool::Free(char *pItem)
{
    assert(pItem != NULL);

    if (pItem >= m_pItem && pItem < m_pItem + m_iItemCnt * uint64_t(m_iItemSize))
    {
        assert(-1 <= m_iHead && m_iHead < m_iItemCnt);

        int iIndex = uint64_t(pItem - m_pItem) / m_iItemSize;
        *GetNext(iIndex) = m_iHead; 
        m_iHead  = iIndex;
        m_iAllocCnt--;
    }
    else
    {
        free(pItem);
    }
}

} // namespace Certain
