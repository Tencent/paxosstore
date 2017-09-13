#ifndef CERTAIN_UTILS_LRUTABLE_H_
#define CERTAIN_UTILS_LRUTABLE_H_

#include "utils/Assert.h"
#include "utils/CircleQueue.h"
#include "utils/FixSizePool.h"

namespace Certain
{

template<typename T>
class clsFixSizeAllocator : public allocator<T>
{
public:
    typedef size_t	size_type;
    typedef T *		pointer;

    clsFixSizePool *m_poFixSizePool;

    template<class Other>
    struct rebind
    {
        typedef clsFixSizeAllocator<Other> other;
    };

    clsFixSizeAllocator()
    {
        m_poFixSizePool = NULL;
    }

    clsFixSizeAllocator(clsFixSizePool *poFixSizePool)
    {
        m_poFixSizePool = poFixSizePool;
    }

    ~clsFixSizeAllocator() { }

    clsFixSizeAllocator(clsFixSizeAllocator<T> const &other)
    {
        m_poFixSizePool = other.m_poFixSizePool;
    }

    clsFixSizeAllocator<T>& operator = (clsFixSizeAllocator<T> const &)
    {
        return (*this);
    }

    template<class Other>
    clsFixSizeAllocator(clsFixSizeAllocator<Other> const &other)
    {
        m_poFixSizePool = other.m_poFixSizePool;
    }

    template<class Other>
    clsFixSizeAllocator<T>& operator = (clsFixSizeAllocator<Other> const &)
    {
        return (*this);
    }

    pointer allocate(size_t count)
    {
        if (m_poFixSizePool == NULL)
        {
            CertainLogFatal("m_poFixSizePool == NULL");
            return NULL;
        }

        return (pointer)m_poFixSizePool->Alloc(count * sizeof(T), false);
    }

    void deallocate(pointer ptr, size_t count)
    {
        if (m_poFixSizePool == NULL)
        {
            CertainLogFatal("m_poFixSizePool == NULL");
            free(ptr);
            return;
        }

        m_poFixSizePool->Free((char *)ptr, false);
    }
};

template<typename T>
bool operator == (const clsFixSizeAllocator<T> &left,
        const clsFixSizeAllocator<T> &right)
{
    return left.m_poFixSizePool == right.m_poFixSizePool;
}

template<typename T>
bool operator != (const clsFixSizeAllocator<T> &left,
        const clsFixSizeAllocator<T> &right)
{
    return left.m_poFixSizePool != right.m_poFixSizePool;
}

template <typename Key_t,
typename Value_t,
typename Hash_t = std::hash<Key_t >,
typename Pred_t = std::equal_to<Key_t > >
class clsLRUTable
{
private:
    struct LRUElt_t;
    typedef pair<const Key_t, LRUElt_t > MemoryType_t;
    typedef clsFixSizeAllocator<MemoryType_t > AllocatorType_t;
    typedef unordered_map<Key_t, LRUElt_t, Hash_t, Pred_t, AllocatorType_t > HashTable_t;
    typedef typename HashTable_t::iterator HashTableIter_t;

    struct LRUElt_t
    {
        Key_t tKey;
        Value_t tValue;
        CIRCLEQ_ENTRY(LRUElt_t) tListEntry;

        LRUElt_t() { }
        LRUElt_t(const Key_t &tArgKey, const Value_t &tArgValue)
            : tKey(tArgKey), tValue(tArgValue)
        {
            CIRCLEQ_ENTRY_INIT(this, tListEntry);
        }
    };

    clsFixSizePool *m_poFixSizePool;
    uint32_t m_iMaxSize;
    bool m_bAutoEliminate;

    HashTable_t *m_ptHashTable;

    CIRCLEQ_HEAD(LRUList_t, LRUElt_t) m_tLRUList;

public:
    clsLRUTable(uint32_t iMaxSize = 1024, bool bAutoEliminate = true)
    {
        m_iMaxSize = iMaxSize;
        m_bAutoEliminate = bAutoEliminate;

        uint32_t iFixCnt = max(m_iMaxSize, 8U);
        m_poFixSizePool = new clsFixSizePool(iFixCnt, sizeof(MemoryType_t));

        AllocatorType_t oAllocator(m_poFixSizePool);

        m_ptHashTable = new HashTable_t(1, Hash_t(), Pred_t(), oAllocator);

        m_ptHashTable->reserve(m_iMaxSize);

        CIRCLEQ_INIT(LRUElt_t, &m_tLRUList);
    }

    virtual ~clsLRUTable()
    {
        delete m_poFixSizePool, m_poFixSizePool = NULL;
        delete m_ptHashTable, m_ptHashTable = NULL;
    }

    size_t Size()
    {
        return m_ptHashTable->size();
    }

    void SetMaxSize(uint32_t iMaxSize)
    {
        m_iMaxSize = iMaxSize;
    }

    bool Add(const Key_t &tKey, const Value_t &tValue)
    {
        bool bRet = false;
        LRUElt_t *ptLRUElt = NULL;
        HashTableIter_t iter = m_ptHashTable->find(tKey);

        if (iter != m_ptHashTable->end())
        {
            CIRCLEQ_REMOVE(LRUElt_t, &m_tLRUList, &iter->second, tListEntry);
            iter->second = LRUElt_t(tKey, tValue);
            ptLRUElt = &(iter->second);
        }
        else
        {
            ptLRUElt = &((*m_ptHashTable)[tKey] = LRUElt_t(tKey, tValue));
            bRet = true;
        }

        CIRCLEQ_INSERT_HEAD(LRUElt_t, &m_tLRUList, ptLRUElt, tListEntry);

        if (m_bAutoEliminate && m_ptHashTable->size() > m_iMaxSize)
        {
            assert(RemoveOldest());
        }

        return bRet;
    }

    bool Remove(const Key_t &tKey)
    {
        HashTableIter_t iter = m_ptHashTable->find(tKey);

        if (iter == m_ptHashTable->end())
        {
            return false;
        }

        CIRCLEQ_REMOVE(LRUElt_t, &m_tLRUList, &iter->second, tListEntry);
        m_ptHashTable->erase(iter);

        return true;
    }

    bool RemoveOldest()
    {
        if (m_ptHashTable->size() == 0)
        {
            return false;
        }

        LRUElt_t *ptLRUElt = CIRCLEQ_LAST(&m_tLRUList);
        assert(Remove(ptLRUElt->tKey));

        return true;
    }

    bool PeekOldest(Key_t &tKey, Value_t &tValue)
    {
        if (m_ptHashTable->size() == 0)
        {
            return false;
        }

        LRUElt_t *ptLRUElt = CIRCLEQ_LAST(&m_tLRUList);
        tKey = ptLRUElt->tKey;
        tValue = ptLRUElt->tValue;

        return true;
    }

    bool Find(const Key_t &tKey, Value_t &tValue)
    {
        HashTableIter_t iter = m_ptHashTable->find(tKey);

        if (iter == m_ptHashTable->end())
        {
            return false;
        }

        tValue = iter->second.tValue;

        return true;
    }

    bool Find(const Key_t &tKey)
    {
        HashTableIter_t iter = m_ptHashTable->find(tKey);

        if (iter == m_ptHashTable->end())
        {
            return false;
        }

        return true;
    }

    bool Refresh(const Key_t &tKey, bool bNewest = true)
    {
        HashTableIter_t iter = m_ptHashTable->find(tKey);

        if (iter == m_ptHashTable->end())
        {
            return false;
        }

        CIRCLEQ_REMOVE(LRUElt_t, &m_tLRUList, &iter->second, tListEntry);
        LRUElt_t *ptLRUElt = &(iter->second);

        if (bNewest)
        {
            CIRCLEQ_INSERT_HEAD(LRUElt_t, &m_tLRUList, ptLRUElt, tListEntry);
        }
        else
        {
            CIRCLEQ_INSERT_TAIL(LRUElt_t, &m_tLRUList, ptLRUElt, tListEntry);
        }

        return true;
    }

    bool IsOverLoad()
    {
        return m_ptHashTable->size() > m_iMaxSize;
    }
};

} // namespace Certain

#endif
