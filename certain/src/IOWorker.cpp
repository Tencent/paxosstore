#include "IOWorker.h"
#include "EntityInfoMng.h"
#include "EntryInfoMng.h"
#include "AsyncPipeMng.h"
#include "CatchUpWorker.h"
#include "PLogWorker.h"
#include <sys/syscall.h>

#if CERTAIN_SIMPLE_EXAMPLE
#include "example/UserWorker.h"
#endif

namespace Certain
{

#define gettid() syscall(__NR_gettid)  

#define IO_SKIP(x) \
do { iCurr += (x); } while(0);
#define IO_ERROR_SKIP(x) \
do { iErrorSkipped += (x); iCurr += (x); } while(0);

int clsNotifyPipe::NotifyByMultiThread()
{
    if (m_bBlockedFlag)
    {
        return 0;
    }

    // It must set m_bBlockedFlag before write.
    if (!__sync_bool_compare_and_swap(&m_bBlockedFlag,
                uint8_t(0), uint8_t(1)))
    {
        return 0;
    }

    while (1)
    {
        int iRet = write(m_iOutFD, "n", 1);
        if (iRet == -1)
        {
            CertainLogFatal("write failed errno %d", errno);

            if (errno == EINTR)
            {
                continue;
            }

            // reset m_bBlockedFlag
            __sync_fetch_and_and(&m_bBlockedFlag, uint8_t(0));

            return -1;
        }

        AssertEqual(iRet, 1);
        break;
    }

    return 0;
}

int clsNotifyPipe::RecvNotify()
{
    int iRet, iFD = clsFDBase::GetFD();
    char acReadBuffer[4];
    int iTotal = 0;

    while (1)
    {
        iRet = read(iFD, acReadBuffer, 4);
        if (iRet == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN)
            {
                break;
            }

            CertainLogFatal("read failed errno %d", errno);
            Assert(false);
        }

        AssertEqual(iRet, 1);
        iTotal += iRet;
    }

    AssertEqual(iTotal, 1);

    // It must reset m_bBlockedFlag after read.
    __sync_fetch_and_and(&m_bBlockedFlag, uint8_t(0));

    return 0;
}

int clsIOWorkerRouter::Init(clsConfigure *poConf)
{
    m_iIOWorkerNum = poConf->GetIOWorkerNum();

    m_iServerNum = poConf->GetServerNum();

    m_poNotifyPipeMng = clsNotifyPipeMng::GetInstance();

    m_poCatchUpWorker = clsCatchUpWorker::GetInstance();

    m_aaIdleWorkerMap = (bool**)malloc(m_iIOWorkerNum * sizeof(bool*));
    m_aaIntChannelCnt = (int32_t**)malloc(m_iIOWorkerNum * sizeof(int32_t*));

    for (uint32_t i = 0; i < m_iIOWorkerNum; ++i)
    {
        m_aaIdleWorkerMap[i] = (bool*)calloc(m_iServerNum, sizeof(bool));
        m_aaIntChannelCnt[i] = (int32_t*)calloc(m_iServerNum, sizeof(int32_t));
    }

    return 0;
}

clsIOWorkerRouter::~clsIOWorkerRouter()
{
    if (m_aaIdleWorkerMap)
    {
        for (uint32_t i = 0; i < m_iIOWorkerNum; ++i)
        {
            free(m_aaIdleWorkerMap[i]);
        }

        free(m_aaIdleWorkerMap);
        m_aaIdleWorkerMap = NULL;
    }

    if (m_aaIntChannelCnt)
    {
        for (uint32_t i = 0; i < m_iIOWorkerNum; ++i)
        {
            free(m_aaIntChannelCnt[i]);
        }

        free(m_aaIntChannelCnt);
        m_aaIntChannelCnt = NULL;
    }
}

uint32_t clsIOWorkerRouter::GetIORspQueueID(clsCmdBase *poCmd)
{
    uint32_t iQueueID;
    clsClientCmd *poClientCmd;

    if (poCmd->GetCmdID() == kPaxosCmd)
    {
        uint32_t iServerID = INVALID_SERVER_ID;
        clsCertainWrapper::GetInstance()->GetCertainUser()->GetServerID(poCmd->GetEntityID(),
                dynamic_cast<clsPaxosCmd *>(poCmd)->GetDestAcceptorID(), iServerID);
        AssertNotEqual(iServerID, INVALID_SERVER_ID);

        iQueueID = SelectWorkerID(iServerID);
    }
    else
    {
        poClientCmd = dynamic_cast<clsClientCmd *>(poCmd);
        iQueueID = poClientCmd->GetIOTracker().iIOWorkerID;
    }

    return iQueueID;
}

int clsIOWorkerRouter::Go(clsCmdBase *poCmd)
{
    int iRet;
    clsIORspQueue *poIORspQueue;
    uint32_t iQueueID = GetIORspQueueID(poCmd);

    poIORspQueue = clsAsyncQueueMng::GetInstance()->GetIORspQueue(
            iQueueID);
    AssertNotEqual(poIORspQueue, NULL);

    iRet = poIORspQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        CertainLogError("PushByMultiThread iQueueID %u ret %d",
                iQueueID, iRet);
        return -1;
    }

    clsNotifyPipe *poPipe = m_poNotifyPipeMng->GetNotifyPipe(iQueueID);
    iRet = poPipe->NotifyByMultiThread();
    if (iRet != 0)
    {
        CertainLogError("NotifyByMultiThread ret %d", iRet);
    }

    return 0;
}

int clsIOWorkerRouter::GoAndDeleteIfFailed(clsCmdBase *poCmd)
{
    int iRet;

    if (poCmd->GetCmdID() == kPaxosCmd)
    {
        clsPaxosCmd *poPaxosCmd = dynamic_cast<clsPaxosCmd *>(poCmd);

        if (poPaxosCmd->GetEntry() < poPaxosCmd->GetMaxChosenEntry()
                || (poPaxosCmd->GetEntry() == poPaxosCmd->GetMaxChosenEntry() && !poPaxosCmd->GetSrcRecord().bChosen))
        {
            iRet = m_poCatchUpWorker->PushCatchUpCmdByMultiThread(poPaxosCmd);
            if (iRet != 0)
            {
                CertainLogError("PushCatchUpCmdByMultiThread ret %d", iRet);
                delete poCmd, poCmd = NULL;
                return -1;
            }

            return 0;
        }
    }

    iRet = Go(poCmd);
    if (iRet != 0)
    {
        CertainLogError("Go E(%lu, %lu) ret %d",
                poCmd->GetEntityID(), poCmd->GetEntry(), iRet);
        delete poCmd, poCmd = NULL;
        return -2;
    }

    return 0;
}

int clsIOWorker::PutToIOReqQueue(clsIOChannel *poChannel,
        clsCmdBase *poCmd)
{
    uint32_t iIOReqQueueID;
    clsIOReqQueue *poIOReqQueue;

    iIOReqQueueID = Hash(poCmd->GetEntityID()) % m_poConf->GetEntityWorkerNum();
    poIOReqQueue = m_poQueueMng->GetIOReqQueue(iIOReqQueueID);

    poCmd->SetIOTracker(IOTracker_t(poChannel->GetFD(), poChannel->GetFDID(),
                m_iWorkerID));

    CertainLogDebug("cmd: %s", poCmd->GetTextCmd().c_str());

#if CERTAIN_SIMPLE_EXAMPLE
    if (poCmd->GetCmdID() == kSimpleCmd)
    {
        clsClientCmd *poClientCmd = dynamic_cast<clsClientCmd *>(poCmd);
        assert(poClientCmd != NULL);
        assert(clsUserWorker::PushUserCmd(poClientCmd) == 0);
        return 0;
    }
#endif

    AssertEqual(poCmd->GetCmdID(), kPaxosCmd);
    int iRet = poIOReqQueue->PushByMultiThread(poCmd);
    if (iRet != 0)
    {
        CertainLogError("PushByMultiThread ret %d cmd %s\n",
                iRet, poCmd->GetTextCmd().c_str());
        return -1;
    }

    return 0;
}

int clsIOWorker::ParseIOBuffer(clsIOChannel *poChannel, char *pcBuffer,
        uint32_t iSize)
{
    uint32_t iCmdCnt = 0;
    uint32_t iErrorSkipped = 0, iCurr = 0;
    clsCmdBase *poCmd;

    CertainLogDebug("conn %s iSize %u",
            poChannel->GetConnInfo().ToString().c_str(), iSize);

    clsCmdFactory *poCmdFactory = clsCmdFactory::GetInstance();

    while (iCurr < iSize)
    {
        if (iCurr + RP_HEADER_SIZE > iSize)
        {
            break;
        }

        RawPacket_t *ptRP = (RawPacket_t *)(pcBuffer + iCurr);
        ConvertToHostOrder(ptRP);

        if (ptRP->hMagicNum != RP_MAGIC_NUM)
        {
            ConvertToNetOrder(ptRP);
            IO_ERROR_SKIP(1);
            continue;
        }

        uint32_t iTotalLen = RP_HEADER_SIZE + ptRP->iLen;
        if (iCurr + iTotalLen > iSize)
        {
            ConvertToNetOrder(ptRP);
            break;
        }

        if (m_poConf->GetEnableCheckSum())
        {
            if (ptRP->iCheckSum == 0)
            {
                // (TODO)rock: add one bit for bHasCheckSum
                CertainLogError("Check if CheckSum disable online");
            }
            else if (ptRP->iCheckSum != CRC32(ptRP->pcData, ptRP->iLen))
            {
                CertainLogFatal("BUG checksum err conn: %s",
                        poChannel->GetConnInfo().ToString().c_str());
                ConvertToNetOrder(ptRP);
                IO_ERROR_SKIP(1);
                continue;
            }
        }

        poCmd = poCmdFactory->CreateCmd(pcBuffer + iCurr, iTotalLen);
        if (poCmd == NULL)
        {
            IO_ERROR_SKIP(iTotalLen);
            continue;
        }

        poCmd->CalcEntityID();
        if (poCmd->GetCmdID() != kPaxosCmd)
        {
            poCmd->SetUUID(poCmdFactory->GetNextUUID());
        }

        poCmd->SetTimestampUS(GetCurrTimeUS());

        int iRet = PutToIOReqQueue(poChannel, poCmd);
        if (iRet != 0)
        {
            IO_ERROR_SKIP(iTotalLen);
            delete poCmd, poCmd = NULL;
        }
        else
        {
            iCmdCnt++;
            IO_SKIP(iTotalLen);
        }
    }

    if (iErrorSkipped > 0 || iCmdCnt > 50)
    {
        CertainLogError("iCmdCnt %u iCurr %d iErrorSkipped %u iSize %u",
                iCmdCnt, iCurr, iErrorSkipped, iSize);
    }

    return iCurr;
}

int clsIOWorker::HandleRead(clsFDBase *poFD)
{
    int iRet, iBytes;
    int iFD = poFD->GetFD();

    clsIOChannel *poChannel = dynamic_cast<clsIOChannel *>(poFD);

    iRet = poChannel->Read(m_pcIOBuffer, CERTAIN_IO_BUFFER_SIZE);
    if (iRet > 0)
    {
        iBytes = ParseIOBuffer(poChannel, m_pcIOBuffer, iRet);
        AssertNotMore(0, iBytes);
        AssertNotMore(iBytes, iRet);

        poChannel->AppendReadBytes(m_pcIOBuffer + iBytes, iRet - iBytes);

        if (!poChannel->IsBroken() && poChannel->IsReadable())
        {
            poChannel->SetReadable(false);
            AssertEqual(iRet, CERTAIN_IO_BUFFER_SIZE);
            AssertEqual(m_poEpollIO->Remove(poChannel), 0);
            AssertEqual(m_poEpollIO->Add(poChannel), 0);

            CertainLogFatal("BUG maybe Check conn %s",
                    poChannel->GetConnInfo().ToString().c_str());
        }
    }
    else
    {
        CertainLogError("poChannel->Read fd %d, ret %d", iFD, iRet);
    }

    if (poChannel->IsBroken())
    {
        CleanBrokenChannel(poChannel);
        return 1;
    }

    return 0;
}

int clsIOWorker::HandleWrite(clsFDBase *poFD)
{
    int iRet;
    int iFD = poFD->GetFD();

    clsIOChannel *poChannel = dynamic_cast<clsIOChannel *>(poFD);

    iRet = poChannel->FlushWriteBuffer();
    if (iRet < 0)
    {
        CertainLogError("poChannel->Write fd %d, ret %d", iFD, iRet);
    }

    if (poChannel->IsBroken())
    {
        CleanBrokenChannel(poChannel);
        return 1;
    }
    else if (!poChannel->IsWritable())
    {
        m_tWritableChannelSet.erase(poChannel);
    }

    return 0;
}

int clsIOWorker::MakeSingleSrvConn(uint32_t iServerID)
{
    int iRet;

    int iFD = CreateSocket(NULL);
    if (iFD == -1)
    {
        CertainLogError("CreateSocket ret %d", iFD);
        return -1;
    }

    ConnInfo_t tConnInfo;
    tConnInfo.iFD = iFD;

    uint32_t iLocalServerID = m_poConf->GetLocalServerID();
    tConnInfo.tLocalAddr = m_vecServerAddr[iLocalServerID];

    tConnInfo.tPeerAddr = m_vecServerAddr[iServerID];

    iRet = Connect(iFD, tConnInfo.tPeerAddr);
    if (iRet != 0)
    {
        CertainLogError("Connect ret %d", iRet);
        return -2;
    }

    CertainLogDebug("try conn: %s", tConnInfo.ToString().c_str());

    clsIOChannel *poChannel = new clsIOChannel(this, tConnInfo,
            iServerID);

    // Flush nego msg when it's writable auto.
    uint8_t acNego[3];
    *(uint16_t *)acNego = htons(RP_MAGIC_NUM);
    acNego[2] = uint8_t(m_iLocalServerID);
    poChannel->AppendWriteBytes((char *)acNego, 3);

    iRet = m_poEpollIO->Add(poChannel);
    if (iRet != 0)
    {
        CertainLogError("m_poEpollIO->Add ret %d", iRet);
        delete poChannel, poChannel = NULL;

        return -3;
    }
    else
    {
        AssertNotEqual(iServerID, INVALID_SERVER_ID);
        m_vecIntChannel[iServerID].push_back(poChannel);
        clsIOWorkerRouter::GetInstance()->GetAndAddIntChannelCnt(m_iWorkerID, iServerID, 1);
    }

    return 0;
}

int clsIOWorker::MakeSrvConn()
{
    uint64_t iCurrTimeMS = GetCurrTimeMS();
    uint32_t iReconnIntvMS = m_poConf->GetReconnIntvMS();
    if (iCurrTimeMS < m_iLastMakeSrvConnTimeMS + iReconnIntvMS)
    {
        return -1;
    }
    m_iLastMakeSrvConnTimeMS = iCurrTimeMS;

    int iRet;
    uint32_t iFailCnt = 0;

    // only server with min num to connect
    for (uint32_t i = 0; i < m_iServerNum; ++i)
    {
        if (i == m_iLocalServerID)
        {
            continue;
        }

        // Compatable to the very old version
        if (m_poConf->GetEnableConnectAll() == 0 && i < m_iLocalServerID)
        {
            continue;
        }

        for (uint32_t j = m_vecIntChannel[i].size(); j < m_iIntConnLimit; ++j)
        {
            iRet = MakeSingleSrvConn(i);
            if (iRet != 0)
            {
                iFailCnt++;
                CertainLogError("InitSingleSrvConn ret %d iServerID %u",
                        iRet, i);
            }
        }
    }

    if (iFailCnt > 0)
    {
        CertainLogError("iFailCnt %u", iFailCnt);
        return -2;
    }

    return 0;
}

void clsIOWorker::CleanBrokenChannel(clsIOChannel *poChannel)
{
    uint32_t iServerID = poChannel->GetServerID();
    CertainLogDebug("iServerID %u conn: %s",
            iServerID, poChannel->GetConnInfo().ToString().c_str());

    if (iServerID != INVALID_SERVER_ID)
    {
        AssertLess(iServerID, m_iServerNum);
        bool bHasErased = false;

        vector<clsIOChannel *>::iterator iter;
        for (iter = m_vecIntChannel[iServerID].begin();
                iter != m_vecIntChannel[iServerID].end(); ++iter)
        {
            if (*iter == poChannel)
            {
                m_vecIntChannel[iServerID].erase(iter);
                int32_t iCheck = clsIOWorkerRouter::GetInstance()->SubAndGetIntChannelCnt(m_iWorkerID, iServerID, 1);
                AssertNotMore(0, iCheck);
                bHasErased = true;
                break;
            }
        }
        Assert(bHasErased);
    }

    m_tWritableChannelSet.erase(poChannel);
    m_poEpollIO->RemoveAndCloseFD(poChannel);
    delete poChannel, poChannel = NULL;
}

void clsIOWorker::RemoveChannel(uint32_t iServerID)
{
    assert(iServerID != INVALID_SERVER_ID);
    assert(iServerID < m_iServerNum);

    vector<clsIOChannel *>::iterator iter;

    uint32_t iCnt = 0;
    for (iter = m_vecIntChannel[iServerID].begin();
            iter != m_vecIntChannel[iServerID].end(); ++iter)
    {
        clsIOChannel * poChannel = *iter;	
        assert(poChannel->GetServerID()==iServerID);

        CertainLogDebug("iServerID %u conn: %s",
                iServerID, poChannel->GetConnInfo().ToString().c_str());

        m_poEpollIO->RemoveAndCloseFD(poChannel);
        delete poChannel, poChannel = NULL;

        iCnt = clsIOWorkerRouter::GetInstance()->SubAndGetIntChannelCnt(m_iWorkerID, iServerID, 1);
    }

    assert(iCnt == 0);
    m_vecIntChannel[iServerID].clear();
    assert(m_vecIntChannel[iServerID].size()==0);

    return;
}

void clsIOWorker::PrintConnInfo()
{
    for(uint32_t i=0; i<m_iServerNum; i++)
    {
        vector<clsIOChannel *>::iterator iter;
        int id = 0; 
        for (iter = m_vecIntChannel[i].begin();
                iter != m_vecIntChannel[i].end(); ++iter)
        {
            clsIOChannel * poChannel = *iter;	
            ConnInfo_t tConnInfo = poChannel->GetConnInfo();
            CertainLogError("svrid %d connectid %d %s\n", i, id, tConnInfo.ToString().c_str());
            id++;
        }
    }

    return;
}
/*
   void clsIOWorker::ReduceChannel()
   {
   for(uint32_t i=0; i<m_iServerNum; i++)
   {
   uint32_t iCnt = m_vecIntChannel[i].size();
   if(iCnt <= 1)
   {
   continue;
   }

   vector<clsIOChannel *>::iterator iter;
   for (iter = m_vecIntChannel[i].begin();
   iter != m_vecIntChannel[i].end(); ++iter)
   {
   clsIOChannel * poChannel = *iter;	
   ConnInfo_t tConnInfo = poChannel->GetConnInfo();
   if(tConnInfo.tLocalAddr.GetNetOrderIP() < tConnInfo.tPeerAddr.GetNetOrderIP())
   {
   continue;
   }

   m_vecIntChannel[i].erase(iter);
   m_poEpollIO->RemoveAndCloseFD(poChannel);
   delete poChannel, poChannel = NULL;
   clsIOWorkerRouter::GetInstance()->SubAndGetIntChannelCnt(m_iWorkerID, i, 1);
   break;
   }
   }
   return;
   }
 */

void clsIOWorker::ServeNewConn()
{
    int iRet;
    uint32_t iConnCnt = 0, iFailCnt = 0, iServerID;
    ConnInfo_t tConnInfo;

    while (1)
    {
        iRet = m_poConnMng->TakeByMultiThread(m_iWorkerID, m_vecIntChannel,
                tConnInfo, iServerID);
        if (iRet != 0)
        {
            break;
        }

        iConnCnt++;

        clsIOChannel *poChannel = new clsIOChannel(this, tConnInfo,
                iServerID, m_iNextFDID);
        m_iNextFDID += m_poConf->GetIOWorkerNum();

        CertainLogImpt("m_iWorkerID %u Serve new conn %s", m_iWorkerID, tConnInfo.ToString().c_str());

        int iRet = m_poEpollIO->Add(poChannel);
        if (iRet != 0)
        {
            CertainLogError("m_poEpollIO->Add ret %d", iRet);
            delete poChannel, poChannel = NULL;

            iFailCnt++;
        }
        else if (iServerID != INVALID_SERVER_ID)
        {
            AssertLess(iServerID, m_iServerNum);
            m_vecIntChannel[iServerID].push_back(poChannel);
            int32_t iCurrCnt = clsIOWorkerRouter::GetInstance()->GetAndAddIntChannelCnt(m_iWorkerID, iServerID, 1);

            if (uint32_t(iCurrCnt) >= m_iIntConnLimit)
            {
                CertainLogZero("m_iWorkerID %u iCurrCnt %d add peer iServerID %u conn %s",
                        m_iWorkerID, iCurrCnt, iServerID, tConnInfo.ToString().c_str());
            }
        }
    }

    // Update s_aaIdleWorkerMap
    for (uint32_t i = 0; i < m_vecIntChannel.size(); ++i)
    {
        clsIOWorkerRouter::GetInstance()->SetIdleWorkerMap(m_iWorkerID, i, false);
        for (uint32_t j = 0; j < m_vecIntChannel[i].size(); ++j)
        {
            if (!m_vecIntChannel[i][j]->IsBufferBusy())
            {
                clsIOWorkerRouter::GetInstance()->SetIdleWorkerMap(m_iWorkerID, i, true);
                break;
            }
        }
    }

    if (iConnCnt > 0)
    {
        CertainLogImpt("iConnCnt %u iFailCnt %u", iConnCnt, iFailCnt);
    }

    if (iFailCnt > 0)
    {
        CertainLogError("iConnCnt %u iFailCnt %u", iConnCnt, iFailCnt);
    }
}

clsIOChannel *clsIOWorker::GetIOChannelByServerID(uint32_t iServerID)
{
    AssertLess(iServerID, m_iServerNum);
    uint32_t iSize = m_vecIntChannel[iServerID].size();
    if (iSize == 0)
    {
        // Avoid too many log when a acceptor down.
        CertainLogDebug("iServerID %u iSize 0", iServerID);
        return NULL;
    }

    uint32_t iChannelSelected = m_vecChannelRouter[iServerID]++;
    iChannelSelected %= iSize;

    clsIOChannel *poChannel = m_vecIntChannel[iServerID][iChannelSelected];
    AssertNotEqual(poChannel, NULL);

    return poChannel;
}

clsIOChannel *clsIOWorker::GetIOChannel(clsCmdBase *poCmd)
{
    if (poCmd->GetCmdID() == kPaxosCmd)
    {
        clsPaxosCmd *poPaxosCmd = dynamic_cast<clsPaxosCmd *>(poCmd);

        uint32_t iDestAcceptorID = poPaxosCmd->GetDestAcceptorID();
        uint64_t iEntityID = poPaxosCmd->GetEntityID();
        uint32_t iServerID = -1;

        AssertEqual(m_poCertainUser->GetServerID(
                    iEntityID, iDestAcceptorID, iServerID), 0);

        return GetIOChannelByServerID(iServerID);
    }
    else
    {
        const IOTracker_t &tIOTracker = poCmd->GetIOTracker();
        clsFDBase *poFD = m_poEpollIO->GetFDBase(tIOTracker.iFD,
                tIOTracker.iFDID);
        if (poFD == NULL)
        {
            CertainLogError("GetFDBase error fd %d fd_id %u",
                    tIOTracker.iFD, tIOTracker.iFDID);
        }
        return dynamic_cast<clsIOChannel *>(poFD);
    }
}

void clsIOWorker::FlushWritableChannel()
{
    uint64_t iFlushTimeoutUS = m_poConf->GetFlushTimeoutUS();

    if (iFlushTimeoutUS > 0)
    {
        uint64_t iCurrTimeUS = GetCurrTimeUS();

        if (m_iNextFlushTimeUS > iCurrTimeUS)
        {
            return;
        }

        m_iNextFlushTimeUS = iCurrTimeUS + iFlushTimeoutUS;
    }
    else
    {
        m_iNextFlushTimeUS = 0;
    }

    set<clsIOChannel *> tNewChannelSet;

    while (m_tWritableChannelSet.size() > 0)
    {
        clsIOChannel *poChannel = *m_tWritableChannelSet.begin();
        m_tWritableChannelSet.erase(poChannel);

        Assert(!poChannel->IsBroken() && poChannel->IsWritable());

        int iRet = poChannel->FlushWriteBuffer();
        if (iRet < 0)
        {
            int iFD = poChannel->GetFD();
            CertainLogError("poChannel->Write fd %d ret %d", iFD, iRet);
        }

        if (poChannel->IsBroken())
        {
            CleanBrokenChannel(poChannel);
        }
        else if (poChannel->IsWritable() && poChannel->GetWriteByteSize() > 0)
        {
            tNewChannelSet.insert(poChannel);
        }
    }

    m_tWritableChannelSet = tNewChannelSet;
}

static clsUseTimeStat *s_poRecoverCmdTimeStat;
static clsUseTimeStat *s_poReadTimeStat;
static clsUseTimeStat *s_poWriteTimeStat;

void clsIOWorker::InitUseTimeStat()
{
    s_poRecoverCmdTimeStat = new clsUseTimeStat("user_Recover");
    s_poReadTimeStat = new clsUseTimeStat("user_Read");
    s_poWriteTimeStat = new clsUseTimeStat("user_Write");
}

void clsIOWorker::PrintUseTimeStat()
{
    if (clsCertainWrapper::GetInstance()->GetConf()->GetEnableTimeStat() == 0)
    {
        return;
    }

    s_poRecoverCmdTimeStat->Print();
    s_poReadTimeStat->Print();
    s_poWriteTimeStat->Print();
}

void clsIOWorker::ConsumeIORspQueue()
{
    int iRet;
    clsIORspQueue *poIORspQueue = m_poQueueMng->GetIORspQueue(m_iWorkerID);

    class clsSerializeCB : public clsIOChannel::clsSerializeCBBase
    {
        private:
            clsCmdBase *m_poCmd;
            int m_iByteSize;
            bool m_bCheckSum;

        public:
            clsSerializeCB(clsCmdBase *poCmd, bool bCheckSum)
                : m_poCmd(poCmd), m_bCheckSum(bCheckSum) { }

            virtual ~clsSerializeCB() { }

            virtual int Call(char *pcBuffer, uint32_t iSize)
            {
                m_iByteSize = m_poCmd->GenRspPkg(pcBuffer, iSize, m_bCheckSum);
                return m_iByteSize;
            }

            int GetByteSize()
            {
                return m_iByteSize;
            }
    };

    while (1)
    {
        clsCmdBase *poCmd = NULL;
        iRet = poIORspQueue->TakeByOneThread(&poCmd);
        if (iRet != 0)
        {
            break;
        }

        if (poCmd->GetCmdID() == kWriteBatchCmd
                || poCmd->GetCmdID() == kRecoverCmd)
        {
            clsClientCmd *poClientCmd = dynamic_cast<clsClientCmd *>(poCmd);
            uint64_t iUseTimeUS = GetCurrTimeUS() - poCmd->GetTimestampUS();

            if (poClientCmd->GetCmdID() == kRecoverCmd)
            {
                s_poRecoverCmdTimeStat->Update(iUseTimeUS);
            }
            else if (poClientCmd->IsReadOnly())
            {
                s_poReadTimeStat->Update(iUseTimeUS);
            }
            else
            {
                s_poWriteTimeStat->Update(iUseTimeUS);
            }

            if (poClientCmd->GetResult() == 0)
            {
                CertainLogInfo("use_time_us %lu cmd: %s res: %d",
                        iUseTimeUS, poClientCmd->GetTextCmd().c_str(),
                        poClientCmd->GetResult());
            }
            else
            {
                CertainLogError("use_time_us %lu cmd: %s res: %d",
                        iUseTimeUS, poClientCmd->GetTextCmd().c_str(),
                        poClientCmd->GetResult());
            }

            uint32_t iPipeIdx = poClientCmd->GetPipeIdx();
            iRet = clsAsyncPipeMng::GetInstance()->SyncWriteByPipeIdx(iPipeIdx,
                    uintptr_t(poClientCmd));
            AssertEqual(iRet, 0);

            continue;
        }

        clsAutoDelete<clsCmdBase> oAuto(poCmd);
        //clsAutoFreeObjPtr<clsPaxosCmd> oAuto(poPaxosCmd, m_poPaxosCmdPool);

        if (poCmd->GetCmdID() == kPaxosCmd)
        {
            clsPaxosCmd *poPaxosCmd = dynamic_cast<clsPaxosCmd *>(poCmd);
            if (poPaxosCmd->GetDestAcceptorID() == INVALID_ACCEPTOR_ID)
            {
                for (uint32_t i = 0; i < m_poConf->GetAcceptorNum(); ++i)
                {
                    if (i == poPaxosCmd->GetSrcAcceptorID())
                    {
                        continue;
                    }

                    poPaxosCmd->SetDestAcceptorID(i);

                    clsIOChannel *poChannel = GetIOChannel(poCmd);
                    if (poChannel == NULL)
                    {
                        CertainLogDebug("cmd_id %d GetIOChannel NULL",
                                poCmd->GetCmdID());
                        continue;
                    }

                    const ConnInfo_t &tConnInfo = poChannel->GetConnInfo();

                    poPaxosCmd->SetDestAcceptorID(INVALID_SERVER_ID);
                    clsSerializeCB oCB(poCmd, m_poConf->GetEnableCheckSum());
                    poChannel->AppendWriteBytes(&oCB);
                    if (poChannel->IsWritable())
                    {
                        m_tWritableChannelSet.insert(poChannel);
                    }

                    CertainLogInfo("conn %s cmd: %s packet_size %d",
                            tConnInfo.ToString().c_str(),
                            poCmd->GetTextCmd().c_str(), oCB.GetByteSize());
                }

                continue;
            }
        }

        clsIOChannel *poChannel = GetIOChannel(poCmd);
        if (poChannel == NULL)
        {
            CertainLogDebug("cmd_id %d GetIOChannel NULL", poCmd->GetCmdID());
            continue;
        }
        const ConnInfo_t &tConnInfo = poChannel->GetConnInfo();

        clsSerializeCB oCB(poCmd, m_poConf->GetEnableCheckSum());
        poChannel->AppendWriteBytes(&oCB);
        if (poChannel->IsWritable())
        {
            m_tWritableChannelSet.insert(poChannel);
        }

        if (poCmd->GetCmdID() != kPaxosCmd)
        {
            clsClientCmd *poClientCmd = dynamic_cast<clsClientCmd *>(poCmd);
            uint64_t iUseTimeUS = GetCurrTimeUS() - poCmd->GetTimestampUS();

            CertainLogInfo("use_time_us %lu conn %s cmd: %s "
                    "res %d packet_size %d",
                    iUseTimeUS, tConnInfo.ToString().c_str(),
                    poClientCmd->GetTextCmd().c_str(),
                    poClientCmd->GetResult(), oCB.GetByteSize());
        }
        else
        {
            CertainLogInfo("cmd: %s packet_size %d",
                    poCmd->GetTextCmd().c_str(), oCB.GetByteSize());
        }
    }

    FlushWritableChannel();
}

void clsIOWorker::AddNotifyPipe()
{
    int iInFD, iOutFD;
    AssertEqual(MakeNonBlockPipe(iInFD, iOutFD), 0);
    AssertEqual(m_poNotifyPipe, NULL);
    m_poNotifyPipe = new clsNotifyPipe(m_poNotifyPipeHandler, iInFD, iOutFD);

    clsNotifyPipeMng *poNotifyPipeMng = clsNotifyPipeMng::GetInstance();
    poNotifyPipeMng->AddNotifyPipe(m_iWorkerID, m_poNotifyPipe);

    AssertEqual(m_poEpollIO->Add(m_poNotifyPipe), 0);
}

void clsIOWorker::UpdateSvrAddr()
{
    vector<Certain::InetAddr_t> tmpServerAddr =  m_poConf->GetServerAddrs();
    assert(tmpServerAddr.size() == m_vecServerAddr.size());

    bool bNeedUpdate = false;

    for(int i=0; i<(int)tmpServerAddr.size(); i++)
    {
        if(tmpServerAddr[i] == m_vecServerAddr[i])
        {
            continue;
        }

        printf("%s replace %s\n", tmpServerAddr[i].ToString().c_str(), m_vecServerAddr[i].ToString().c_str());
        RemoveChannel(i);
        bNeedUpdate = true;
    }

    if(bNeedUpdate)
    {
        m_vecServerAddr = tmpServerAddr;
    }
}

void clsIOWorker::Run()
{
    int cpu_cnt = GetCpuCount();

    if (cpu_cnt == 48)
    {
        SetCpu(8, cpu_cnt);
    }
    else
    {
        SetCpu(4, cpu_cnt);
    }

    SetThreadTitle("io_%u_%u", m_iLocalServerID, m_iWorkerID);
    CertainLogInfo("io_%u_%u run", m_iLocalServerID, m_iWorkerID);

    static __thread uint64_t llLoop = 0;

    while (1)
    {
        if (CheckIfExiting(1000))
        {
            printf("io_%u_%u exit\n", m_iLocalServerID, m_iWorkerID);
            CertainLogInfo("io_%u_%u exit", m_iLocalServerID, m_iWorkerID);
            break;
        }

        if((llLoop++) % 100 == 0)
        {
            UpdateSvrAddr();

            if(llLoop % 1000 == 0)
            {
                PrintConnInfo();
            }

            //ReduceChannel();
        }

        MakeSrvConn();
        ServeNewConn();

        ConsumeIORspQueue();

        m_poEpollIO->RunOnce(1);
    }
}

} // namespace Certain
