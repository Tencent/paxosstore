#ifndef CERTAIN_UTILS_CircleQUEUE_H_
#define CERTAIN_UTILS_CircleQUEUE_H_

#include "utils/Assert.h"
#include "utils/Random.h"

namespace Certain
{

typedef void * CircleQueueElt_t;

template<typename Type>
class clsCircleQueue
{
private:
    volatile unsigned long m_ulHead;
    volatile unsigned long m_ulTail;

    uint32_t m_iSize;
    Type *m_ptElt;

    int PushByMultiThreadInner(Type tElt);

public:
    clsCircleQueue();
    clsCircleQueue(uint32_t iSize);
    ~clsCircleQueue();

    void Resize(uint32_t iSize);
    bool IsFull();
    uint32_t Size();

    int TakeByOneThread(Type *ptElt);
    int MultiTakeByOneThread(Type *ptElt, int iMaxCnt);
    int PushByMultiThread(Type tElt, uint32_t iRetryTime = 5);

    // Use for single thread.
    int Take(Type *ptElt);
    int Push(Type tElt);
};

template<typename Type>
clsCircleQueue<Type>::clsCircleQueue()
{
    m_ulHead = 0;
    m_ulTail = 0;

    m_iSize = 0;
    m_ptElt = NULL;

    assert(sizeof(Type) <= sizeof(uintptr_t));
}

template<typename Type>
clsCircleQueue<Type>::clsCircleQueue(uint32_t iSize)
{
    AssertNotEqual(iSize, 0);

    m_ulHead = 0;
    m_ulTail = 0;

    m_iSize = iSize;
    m_ptElt = (Type *)calloc(iSize, sizeof(Type));
    AssertNotEqual(m_ptElt, NULL);
}

template<typename Type>
clsCircleQueue<Type>::~clsCircleQueue()
{
    free(m_ptElt), m_ptElt = NULL;
}

template<typename Type>
void clsCircleQueue<Type>::Resize(uint32_t iSize)
{
    AssertNotEqual(iSize, 0);

    m_ulHead = 0;
    m_ulTail = 0;

    if (m_ptElt != NULL) free(m_ptElt), m_ptElt = NULL;

    m_iSize = iSize;
    m_ptElt = (Type *)malloc(iSize * sizeof(Type));
    AssertNotEqual(m_ptElt, NULL);
}

template<typename Type>
bool clsCircleQueue<Type>::IsFull()
{
    return m_ulHead - m_ulTail >= m_iSize;
}

template<typename Type>
uint32_t clsCircleQueue<Type>::Size()
{
    return m_ulHead - m_ulTail;
}

template<typename Type>
int clsCircleQueue<Type>::TakeByOneThread(Type *ptElt)
{
    AssertNotEqual(m_iSize, 0);

    if (m_ulHead <= m_ulTail)
    {
        return -1;
    }

    *ptElt = m_ptElt[m_ulTail % m_iSize];
    if (*ptElt == 0)
    {
        return -2;
    }

    m_ptElt[m_ulTail % m_iSize] = 0;

    m_ulTail++;

    return 0;
}

template<typename Type>
int clsCircleQueue<Type>::MultiTakeByOneThread(Type *ptElt, int iMaxCnt)
{
    AssertNotEqual(m_iSize, 0);

    if (m_ulHead <= m_ulTail)
    {
        return -1;
    }

    unsigned long iCnt = m_ulHead - m_ulTail;
    iMaxCnt = (iCnt <= (unsigned long)iMaxCnt ? iCnt : iMaxCnt);

    for(int i=0; i<iMaxCnt; i++)
    {
        ptElt[i] = m_ptElt[m_ulTail % m_iSize];

        if(ptElt[i] == 0)
        {
            return i;
        }

        m_ptElt[m_ulTail % m_iSize] = 0;
        m_ulTail++;
    }

    return iMaxCnt;
}

template<typename Type>
int clsCircleQueue<Type>::PushByMultiThread(Type tElt, uint32_t iRetryTime)
{
#if CERTAIN_DEBUG
    static __thread clsRandom *__poRandom = new clsRandom(uint32_t(pthread_self()));
    if (__poRandom->Next() % 10 == 0) {
        return -7999;
    }
#endif

    int iFinalRet = -1;

    for (uint32_t i = 0; i < iRetryTime; ++i)
    {
        int iRet = PushByMultiThreadInner(tElt);

        if (iRet == 0)
        {
            return 0;
        }

        if (iRet == -2)
        {
            iFinalRet = -2;
        }
    }

    return iFinalRet;
}

template<typename Type>
int clsCircleQueue<Type>::PushByMultiThreadInner(Type tElt)
{
    AssertNotEqual(m_iSize, 0);
    AssertNotEqual(tElt, 0);

    volatile unsigned long ulHead = m_ulHead;

    if (ulHead >= m_iSize + m_ulTail)
    {
        return -1;
    }

    if (__sync_bool_compare_and_swap(&m_ulHead, ulHead, ulHead + 1))
    {
        m_ptElt[ulHead % m_iSize] = tElt;
        return 0;
    }

    return -2;
}

template<typename Type>
int clsCircleQueue<Type>::Take(Type *ptElt)
{
    if (m_ulHead <= m_ulTail)
    {
        return -1;
    }

    *ptElt = m_ptElt[m_ulTail % m_iSize];
    m_ulTail++;

    return 0;
}

template<typename Type>
int clsCircleQueue<Type>::Push(Type tElt)
{
    if (m_ulHead >= m_iSize + m_ulTail)
    {
        return -1;
    }

    m_ptElt[m_ulHead % m_iSize] = tElt;
    m_ulHead++;

    return 0;
}

// An implemetation of embedable circle queue.

// Circular queue definitions.

#define	CIRCLEQ_HEAD(name, type) \
	struct name { \
		type *cqh_first; \
		type *cqh_last; \
	}

#define	CIRCLEQ_ENTRY(type) \
	struct { \
		type *cqe_next; \
		type *cqe_prev; \
	}

// Circular queue functions.

#define	CIRCLEQ_INIT(type, head) do { \
		(head)->cqh_first = (type *)(head); \
		(head)->cqh_last = (type *)(head); \
	} while (0)

#define	CIRCLEQ_ENTRY_INIT(elm, field) do { \
		(elm)->field.cqe_next = NULL; \
		(elm)->field.cqe_prev = NULL; \
	} while (0)

#define IS_ENTRY_IN_CIRCLEQ(elm, field) \
	((elm)->field.cqe_next != NULL && (elm)->field.cqe_prev != NULL)

#define	CIRCLEQ_INSERT_HEAD(type, head, elm, field) do { \
		(elm)->field.cqe_next = (head)->cqh_first; \
		(elm)->field.cqe_prev = (type *)(head); \
		if ((head)->cqh_last == (type *)(head)) \
			(head)->cqh_last = (elm); \
		else \
			(head)->cqh_first->field.cqe_prev = (elm); \
		(head)->cqh_first = (elm); \
} while (0)

#define	CIRCLEQ_INSERT_TAIL(type, head, elm, field) do { \
		(elm)->field.cqe_next = (type *)(head); \
		(elm)->field.cqe_prev = (head)->cqh_last; \
		if ((head)->cqh_first == (type *)(head)) \
			(head)->cqh_first = (elm); \
		else \
			(head)->cqh_last->field.cqe_next = (elm); \
		(head)->cqh_last = (elm); \
} while (0)

#define	CIRCLEQ_REMOVE(type, head, elm, field) do { \
		if ((elm)->field.cqe_next == (type *)(head)) \
			(head)->cqh_last = (elm)->field.cqe_prev; \
		else \
			(elm)->field.cqe_next->field.cqe_prev = \
			    (elm)->field.cqe_prev; \
		if ((elm)->field.cqe_prev == (type *)(head)) \
			(head)->cqh_first = (elm)->field.cqe_next; \
		else \
			(elm)->field.cqe_prev->field.cqe_next = \
			    (elm)->field.cqe_next; \
} while (0)

// Circular queue access methods.

#define	CIRCLEQ_EMPTY(type, head)		((head)->cqh_first == (type *)(head))
#define	CIRCLEQ_FIRST(head)		((head)->cqh_first)
#define	CIRCLEQ_LAST(head)		((head)->cqh_last)

} // namespace Certain

#endif
