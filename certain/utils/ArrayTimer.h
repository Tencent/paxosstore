#ifndef CERTAIN_UTILS_ARRAYTIMER_H_
#define CERTAIN_UTILS_ARRAYTIMER_H_

#include "utils/Assert.h"
#include "utils/Time.h"
#include "utils/CircleQueue.h"

namespace Certain
{

template<typename TimeoutElt_t >
class clsArrayTimer
{
public:
    struct TimeoutEntry_t
    {
        uint32_t iArrayIndex;
        CIRCLEQ_ENTRY(TimeoutElt_t) tEntry;
    };

private:
    uint32_t m_iTimeoutCnt;
    uint32_t m_iReadyCnt;

    uint32_t m_iMaxTimeoutMS;
    uint32_t m_iCurrPos;
    uint64_t m_iCurrTimeMS;

    CIRCLEQ_HEAD(TimeoutList_t, TimeoutElt_t);

    TimeoutList_t *m_ptTimeoutList;

    void MoveTimeoutToReadyList(uint64_t iCurrTimeMS)
    {
        uint32_t iReadyNum = 0;
        if (iCurrTimeMS > m_iCurrTimeMS)
        {
            iReadyNum = iCurrTimeMS - m_iCurrTimeMS;
        }
        m_iCurrTimeMS = iCurrTimeMS;

        if (iReadyNum == 0 || m_iTimeoutCnt == 0)
        {
            return;
        }

        if (iReadyNum > m_iMaxTimeoutMS)
        {
            iReadyNum = m_iMaxTimeoutMS;
        }

        TimeoutList_t *ptReadyList = &m_ptTimeoutList[m_iMaxTimeoutMS];

        for (uint32_t i = 0; i < iReadyNum; ++i)
        {
            TimeoutList_t *ptCurrList = &m_ptTimeoutList[m_iCurrPos];

            while (!CIRCLEQ_EMPTY(TimeoutElt_t, ptCurrList))
            {
                TimeoutElt_t *ptLast = CIRCLEQ_LAST(ptCurrList);

                CIRCLEQ_REMOVE(TimeoutElt_t, ptCurrList, ptLast,
                        tTimeoutEntry.tEntry);
                AssertEqual(ptLast->tTimeoutEntry.iArrayIndex, m_iCurrPos);
                m_iTimeoutCnt--;

                CIRCLEQ_INSERT_HEAD(TimeoutElt_t, ptReadyList, ptLast,
                        tTimeoutEntry.tEntry);
                ptLast->tTimeoutEntry.iArrayIndex = m_iMaxTimeoutMS;
                m_iReadyCnt++;
            }

            m_iCurrPos++;
            if (m_iCurrPos == m_iMaxTimeoutMS)
            {
                m_iCurrPos = 0;
            }
        }

    }

public:
    clsArrayTimer(uint32_t iMaxTimeoutMS)
    {
        m_iTimeoutCnt = 0;
        m_iReadyCnt = 0;
        m_iMaxTimeoutMS = iMaxTimeoutMS;

        // m_ptTimeoutList[m_iMaxTimeoutMS] used as the ready list.
        m_ptTimeoutList = (TimeoutList_t *)calloc(m_iMaxTimeoutMS + 1,
                sizeof(TimeoutList_t));
        AssertNotEqual(m_ptTimeoutList, NULL);

        for (uint32_t i = 0; i < iMaxTimeoutMS + 1; ++i)
        {
            CIRCLEQ_INIT(TimeoutElt_t, &m_ptTimeoutList[i]);
        }

        m_iCurrPos = 0;
        m_iCurrTimeMS = GetCurrTimeMS();
    }

    ~clsArrayTimer()
    {
        free(m_ptTimeoutList), m_ptTimeoutList = NULL;
    }

    size_t Size()
    {
        return m_iTimeoutCnt + m_iReadyCnt;
    }

    bool Add(TimeoutElt_t *ptElt, uint32_t iTimeoutMS)
    {
        assert(iTimeoutMS > 0 && iTimeoutMS <= m_iMaxTimeoutMS);

        if (Exist(ptElt))
        {
            return false;
        }

        uint64_t iCurrTimeMS = GetCurrTimeMS();
        if (iCurrTimeMS < m_iCurrTimeMS)
        {
            CertainLogFatal("m_iCurrTimeMS %lu %lu is not increasing",
                    m_iCurrTimeMS, iCurrTimeMS);
            // Fix it.
            iCurrTimeMS = m_iCurrTimeMS;
        }

        MoveTimeoutToReadyList(iCurrTimeMS);

        uint32_t iPos = m_iCurrPos + iTimeoutMS - 1;
        if (iPos >= m_iMaxTimeoutMS)
        {
            iPos -= m_iMaxTimeoutMS;
        }
        AssertLess(iPos, m_iMaxTimeoutMS);

        CIRCLEQ_INSERT_HEAD(TimeoutElt_t, &m_ptTimeoutList[iPos], ptElt,
                tTimeoutEntry.tEntry);
        ptElt->tTimeoutEntry.iArrayIndex = iPos;

        m_iTimeoutCnt++;

        return true;
    }

    bool Remove(TimeoutElt_t *ptElt)
    {
        if (!Exist(ptElt))
        {
            AssertEqual(ptElt->tTimeoutEntry.iArrayIndex, 0);
            return false;
        }

        TimeoutList_t *ptList = NULL;
        ptList = &m_ptTimeoutList[ptElt->tTimeoutEntry.iArrayIndex];

        CIRCLEQ_REMOVE(TimeoutElt_t, ptList, ptElt, tTimeoutEntry.tEntry);

        if (ptElt->tTimeoutEntry.iArrayIndex < m_iMaxTimeoutMS)
        {
            m_iTimeoutCnt--;
        }
        else
        {
            m_iReadyCnt--;
        }

        ptElt->tTimeoutEntry.iArrayIndex = 0;
        CIRCLEQ_ENTRY_INIT(ptElt, tTimeoutEntry.tEntry);

        return true;
    }

    TimeoutElt_t *TakeTimeoutElt()
    {
        if (m_iReadyCnt == 0)
        {
            uint64_t iCurrTimeMS = GetCurrTimeMS();
            MoveTimeoutToReadyList(iCurrTimeMS);

            if (m_iReadyCnt == 0)
            {
                return NULL;
            }
        }

        TimeoutList_t *ptReadyList = &m_ptTimeoutList[m_iMaxTimeoutMS];
        TimeoutElt_t *ptElt = CIRCLEQ_LAST(ptReadyList);
        CIRCLEQ_REMOVE(TimeoutElt_t, ptReadyList, ptElt, tTimeoutEntry.tEntry);

        m_iReadyCnt--;

        ptElt->tTimeoutEntry.iArrayIndex = 0;
        CIRCLEQ_ENTRY_INIT(ptElt, tTimeoutEntry.tEntry);

        return ptElt;
    }

    bool Exist(TimeoutElt_t *ptElt)
    {
        return IS_ENTRY_IN_CIRCLEQ(ptElt, tTimeoutEntry.tEntry);
    }
};

} // namespace Certain

#endif
