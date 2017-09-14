#include "Configure.h"

namespace Certain
{

int clsConfigure::ParseByLine(string strLine, string &strKey,
        string &strValue)
{
    string strFinal;
    for (uint32_t i = 0; i < strLine.size(); ++i)
    {
        char ch = strLine[i];
        if (ch == ' ' || ch == '\t' || ch == '\n')
        {
            continue;
        }

        strFinal += ch;
    }

    if (strFinal.size() == 0 || strFinal[0] == '#')
    {
        return 1;
    }

    size_t iFound = strFinal.find('=');
    if (iFound == string::npos)
    {
        CertainLogFatal("no = in %s", strLine.c_str());
        return -1;
    }

    strKey = strFinal.substr(0, iFound);
    strValue = strFinal.substr(iFound + 1);

    return 0;
}

int clsConfigure::LoadDefaultValue()
{
    map<string, clsValueBase *>::iterator iter;
    for (iter = m_tKeyMap.begin();
            iter != m_tKeyMap.end(); ++iter)
    {
        clsValueBase *poType = iter->second;
        poType->LoadDefaultValue();
    }
    return 0;
}

string clsConfigure::s_strConfSuffix = "";

int clsConfigure::LoadFromFile(const char *pcFilePath)
{
    if (pcFilePath != NULL)
    {
        m_strFilePath = pcFilePath;
    }
    if (m_strFilePath.size() == 0)
    {
        CertainLogFatal("file path size 0");
        return -1;
    }

    int iRet;
    ifstream fin(m_strFilePath.c_str());
    if (fin.bad() || fin.fail())
    {
        fin.close();
        CertainLogFatal("open %s fail", m_strFilePath.c_str());
        return -2;
    }

    string strLine, strKey, strValue;
    while (getline(fin, strLine))
    {
        iRet = ParseByLine(strLine, strKey, strValue);
        if (iRet == 1)
        {
            // comment or empty line
            continue;
        }
        else if (iRet != 0)
        {
            CertainLogFatal("ParseByLine ret %d line: %s",
                    iRet, strLine.c_str());
            return -3;
        }

        bool bUseSuffix = false;
        if (s_strConfSuffix.size() > 0 && strKey.size() > s_strConfSuffix.size())
        {
            string strSuffix = strKey.substr(strKey.size() - s_strConfSuffix.size());
            if (strSuffix == s_strConfSuffix)
            {
                strKey = strKey.substr(0, strKey.size() - s_strConfSuffix.size());
                bUseSuffix = true;
            }
        }

        if (m_tKeyMap.find(strKey) == m_tKeyMap.end())
        {
            CertainLogError("key %s not found", strKey.c_str());
            continue;
        }

        clsValueBase *poValue = m_tKeyMap[strKey];
        if (!bUseSuffix && poValue->IsUseSuffix())
        {
            CertainLogError("prior to use suffix %s : %s",
                    strKey.c_str(), strValue.c_str());
            continue;
        }

        if (bUseSuffix)
        {
            poValue->SetUseSuffix(bUseSuffix);
        }

        iRet = poValue->Parse(strValue);
        if (iRet != 0)
        {
            CertainLogFatal("Parse ret %d value %s", iRet, strValue.c_str());
            return -4;
        }
    }

    fin.close();
    return 0;
}

int clsConfigure::LoadFromOption(int iArgc, char *pArgv[])
{
    if (iArgc == 0)
    {
        return 0;
    }

    int iRet = 0, opt;
    clsValueBase *poValue;
    optind = 1;

    // The configures parsed here must not occur in UpdateConf.
    while ((opt = getopt(iArgc, pArgv, "c:l:n:i:a:e:s:")) != -1)
    {
        switch(opt)
        {
            case 'c':
                m_strFilePath = optarg;
                break;

            case 'l':
                m_strLogPath = optarg;
                break;

            case 'n':
                poValue = m_tKeyMap["AcceptorNum"];
                poValue->Parse(optarg);
                break;

            case 'i':
                poValue = m_tKeyMap["LocalServerID"];
                poValue->Parse(optarg);
                break;

            case 'a':
                poValue = m_tKeyMap["ServerAddrs"];
                iRet = poValue->Parse(optarg);
                break;

            case 'e':
                poValue = m_tKeyMap["ExtAddr"];
                poValue->Parse(optarg);
                break;

            case 's':
                s_strConfSuffix = optarg;
                break;
        }

        if (iRet != 0)
        {
            CertainLogFatal("optarg %s ret %d", optarg, iRet);
            return -1;
        }
    }

    if (m_strFilePath.size() == 0)
    {
        return -2;
    }

    return 0;
}

void clsConfigure::AddUint32(uint32_t &iUint32, string strKey,
        uint32_t iDefault)
{
    clsValueUint32 *poUint32 = new clsValueUint32(strKey, &iUint32, iDefault);
    m_tKeyMap[strKey] = dynamic_cast<clsValueBase *>(poUint32);
}

void clsConfigure::AddString(string &strString, string strKey,
        string strDefault)
{
    clsValueString *poString = new clsValueString(strKey, &strString,
            strDefault);
    m_tKeyMap[strKey] = dynamic_cast<clsValueBase *>(poString);
}

void clsConfigure::AddInetAddr(InetAddr_t &tInetAddr, string strKey,
        InetAddr_t tDefault)
{
    clsValueInetAddr *poInetAddr = new clsValueInetAddr(strKey, &tInetAddr,
            tDefault);
    m_tKeyMap[strKey] = dynamic_cast<clsValueBase *>(poInetAddr);
}

void clsConfigure::AddInetAddrs(vector<InetAddr_t> &vecInetAddrs,
        string strKey, vector<InetAddr_t> vecDefault)
{
    clsValueInetAddrs *poInetAddrs = new clsValueInetAddrs(strKey,
            &vecInetAddrs, vecDefault);
    m_tKeyMap[strKey] = dynamic_cast<clsValueBase *>(poInetAddrs);
}

int clsConfigure::AddVariables()
{
#define ADD_UINT32(var, iDefault) AddUint32(m_i##var, #var, iDefault)
    ADD_UINT32(OSSIDKey, CERTAIN_OSS_DEFAULT_ID_KEY);
    ADD_UINT32(PLogType, 0);
    ADD_UINT32(AcceptorNum, 3);
    ADD_UINT32(LocalServerID, 0);
    ADD_UINT32(ServerNum, 0);
    ADD_UINT32(ReconnIntvMS, 1000);
    ADD_UINT32(EntityWorkerNum, 1);
    ADD_UINT32(IOWorkerNum, 1);
    ADD_UINT32(PLogWorkerNum, 1);
    ADD_UINT32(DBWorkerNum, 1);
    ADD_UINT32(GetAllWorkerNum, 1);
    ADD_UINT32(IOQueueSize, 10000);
    ADD_UINT32(PLogQueueSize, 2000);
    ADD_UINT32(DBQueueSize, 2000);
    ADD_UINT32(CatchUpQueueSize, 100000);
    ADD_UINT32(GetAllQueueSize, 400000);
    ADD_UINT32(IntConnLimit, 1);
    ADD_UINT32(EnablePreAuth, 1);
    ADD_UINT32(MaxCatchUpSpeedKB, 4096);
    ADD_UINT32(MaxCatchUpNum, 10);
    ADD_UINT32(MaxCatchUpConcurr, 10000);
    ADD_UINT32(MaxCommitNum, 10);
    ADD_UINT32(UseCertainLog, 1);
    ADD_UINT32(UseConsole, 0);
    ADD_UINT32(LogLevel, 4);
    ADD_UINT32(UsePerfLog, 0);
    ADD_UINT32(CmdTimeoutMS, 1000);
    ADD_UINT32(RecoverTimeoutMS, 1000);
    ADD_UINT32(LocalAcceptFirst, 0);
    ADD_UINT32(MaxEntityBitNum, 64);
    ADD_UINT32(MaxMemEntityNum, 10000000);
    ADD_UINT32(MaxMemEntryNum, 5000000);
    ADD_UINT32(MaxLeaseMS, 0);
    ADD_UINT32(MinLeaseMS, 0);
    ADD_UINT32(EnableCheckSum, 0);
    ADD_UINT32(DBRoutineCnt, 5);
    ADD_UINT32(GetAllRoutineCnt, 5);
    ADD_UINT32(DBCtrlPLogGetCnt, 10);
    ADD_UINT32(DBCtrlSleepMS, 1);
    ADD_UINT32(EnableAutoFixEntry, 0);
    ADD_UINT32(GetAllMaxNum, 0);
    ADD_UINT32(EnableMaxPLogEntry, 0);
    ADD_UINT32(EnableGetAllOnly, 0);
    ADD_UINT32(PLogRoutineCnt, 5);
    ADD_UINT32(IOReqTimeoutMS, 0);
    ADD_UINT32(FlushTimeoutUS, 100);

    ADD_UINT32(UseDBBatch, 0);
    ADD_UINT32(DBBatchCnt, 20);

    ADD_UINT32(UsePLogWriteWorker, 0);
    ADD_UINT32(PLogWriteWorkerNum, 2);
    ADD_UINT32(PLogWriteQueueSize, 100000);
    ADD_UINT32(PLogWriteTimeoutUS, 100);
    ADD_UINT32(PLogWriteMaxNum, 100);

    ADD_UINT32(WakeUpTimeoutUS, 500);
    ADD_UINT32(EnableTimeStat, 0);
    ADD_UINT32(MaxEmbedValueSize, 0);

    ADD_UINT32(MaxMultiCmdSizeForC, 0);
    ADD_UINT32(MaxMultiCmdStartHour, 21);
    ADD_UINT32(MaxMultiCmdHourCnt, 2);

    ADD_UINT32(EnableLearnOnly, 0);

    ADD_UINT32(MaxCatchUpCnt, 10000);

    ADD_UINT32(MaxMemCacheSizeMB, 2000);

    ADD_UINT32(EnableConnectAll, 1);

    ADD_UINT32(UseIndexHash, 0);

    ADD_UINT32(RandomDropRatio, 0);

    AddString(m_strCertainPath, "CertainPath", "/home/rockzheng/certain");
    AddString(m_strPerfLogPath, "PerfLogPath", "/home/rockzheng/certain/perflog");
    AddString(m_strLogPath, "LogPath", "/home/rockzheng/certain/log");

    vector<InetAddr_t> vecDefault;
    AddInetAddrs(m_vecServerAddr, "ServerAddrs",
            vecDefault);

    InetAddr_t tDefaultExtAddr;
    AddInetAddr(m_tExtAddr, "ExtAddr", tDefaultExtAddr);

    return 0;
}

void clsConfigure::RemoveVariables()
{
    map<string, clsValueBase *>::iterator iter;
    for (iter = m_tKeyMap.begin();
            iter != m_tKeyMap.end(); ++iter)
    {
        delete iter->second, iter->second = NULL;
    }
    m_tKeyMap.clear();
}

void clsConfigure::PrintAll()
{
    printf("s_strConfSuffix %s\n", s_strConfSuffix.c_str());
    CertainLogImpt("s_strConfSuffix %s", s_strConfSuffix.c_str());

    map<string, clsValueBase *>::iterator iter;
    for (iter = m_tKeyMap.begin();
            iter != m_tKeyMap.end(); ++iter)
    {
        clsValueBase *poValue = iter->second;
        poValue->Print();
    }
}

void clsConfigure::UpdateConf(clsConfigure *poNewConf)
{
    // Add Conf that need load in real time.

#define UPDATE_UINT32_CONF(name) \
    do { \
        uint32_t iOld = m_i##name; \
        m_i##name = poNewConf->Get##name(); \
        if (iOld != m_i##name) \
        { \
            CertainLogZero("Update %s succ %u -> %u", \
#name, iOld, m_i##name); \
        } \
    } while (0);

    UPDATE_UINT32_CONF(DBCtrlPLogGetCnt);
    UPDATE_UINT32_CONF(DBCtrlSleepMS);
    UPDATE_UINT32_CONF(MaxCatchUpSpeedKB);
    UPDATE_UINT32_CONF(MaxCatchUpConcurr);
    UPDATE_UINT32_CONF(MaxMemEntityNum);
    UPDATE_UINT32_CONF(FlushTimeoutUS);
    UPDATE_UINT32_CONF(MaxLeaseMS);
    UPDATE_UINT32_CONF(MinLeaseMS);
    UPDATE_UINT32_CONF(EnableAutoFixEntry);
    UPDATE_UINT32_CONF(GetAllMaxNum);
    UPDATE_UINT32_CONF(EnableMaxPLogEntry);
    UPDATE_UINT32_CONF(EnableGetAllOnly);
    UPDATE_UINT32_CONF(CmdTimeoutMS);
    UPDATE_UINT32_CONF(RecoverTimeoutMS);
    UPDATE_UINT32_CONF(IOReqTimeoutMS);

    UPDATE_UINT32_CONF(UseDBBatch);
    UPDATE_UINT32_CONF(DBBatchCnt);

    UPDATE_UINT32_CONF(UsePLogWriteWorker);
    UPDATE_UINT32_CONF(PLogWriteTimeoutUS);
    UPDATE_UINT32_CONF(PLogWriteMaxNum);

    UPDATE_UINT32_CONF(WakeUpTimeoutUS);
    UPDATE_UINT32_CONF(EnableTimeStat);
    UPDATE_UINT32_CONF(MaxEmbedValueSize);

    UPDATE_UINT32_CONF(MaxMultiCmdSizeForC);
    UPDATE_UINT32_CONF(MaxMultiCmdStartHour);
    UPDATE_UINT32_CONF(MaxMultiCmdHourCnt);

    UPDATE_UINT32_CONF(EnableLearnOnly);

    UPDATE_UINT32_CONF(MaxCatchUpCnt);

    UPDATE_UINT32_CONF(MaxMemCacheSizeMB);
    UPDATE_UINT32_CONF(EnableConnectAll);
    UPDATE_UINT32_CONF(UseIndexHash);

    UPDATE_UINT32_CONF(RandomDropRatio);
}

void clsConfigure::LoadAgain()
{
    clsConfigure oConf(m_strFilePath.c_str());
    int iRet = oConf.LoadFromFile();
    if (iRet != 0)
    {
        CertainLogFatal("LoadFromFile ret %d", iRet);
        return;
    }
    UpdateConf(&oConf);
}

} // namespace Certain
