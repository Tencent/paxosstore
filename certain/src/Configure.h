#ifndef CERTAIN_CONFIGURE_H_
#define CERTAIN_CONFIGURE_H_

#include "utils/Assert.h"
#include "network/SocketHelper.h"

namespace Certain
{

enum enumValueType
{
    UINT32 = 0,
    STRING,
    INET_ADDR,
    INET_ADDRS
};

class clsValueBase
{
private:
    enumValueType m_eType;
    bool m_bUseSuffix;

public:
    clsValueBase(enumValueType eType) : m_eType(eType), m_bUseSuffix(false) { }
    virtual ~clsValueBase() { }

    enumValueType GetType() { return m_eType; }

    BOOLEN_IS_SET(UseSuffix);

    virtual void LoadDefaultValue() = 0;
    virtual void Print() = 0;
    virtual int Parse(string strValue) = 0;
};

template<typename T, enumValueType eType>
    class clsValue : public clsValueBase
{
private:
    string m_strKey;

    T *m_ptValue;
    T m_tDefaultValue;

public:
    clsValue() : clsValueBase(eType) { }

    clsValue(string strKey, T *ptValue, T tDefaultValue) :
        clsValueBase(eType),
        m_strKey(strKey),
        m_ptValue(ptValue),
        m_tDefaultValue(tDefaultValue) { }

    virtual ~clsValue() { }

    string GetKey() { return m_strKey; }
    T *GetValuePtr() { return m_ptValue; }
    T GetDefaultValue() { return m_tDefaultValue; }

    virtual void LoadDefaultValue() { *m_ptValue = m_tDefaultValue; }
    virtual void Print() = 0;
    virtual int Parse(string strValue) = 0;
};

class clsValueUint32 : public clsValue<uint32_t, UINT32>
{
public:
    clsValueUint32() { }

    clsValueUint32(string strKey, uint32_t *ptValue, uint32_t tDefaultValue)
        : clsValue(strKey, ptValue, tDefaultValue) { }

    virtual ~clsValueUint32() { }

    void Print()
    {
        string strKey = GetKey();
        uint32_t iValue = *GetValuePtr();

        printf("%s %u\n", strKey.c_str(), iValue);
        CertainLogImpt("%s %u", strKey.c_str(), iValue);
    }
    int Parse(string strValue)
    {
        *GetValuePtr() = strtoull(strValue.c_str(), NULL, 10);
        return 0;
    }
};

class clsValueString : public clsValue<string, STRING>
{
public:
    clsValueString() { }

    clsValueString(string strKey, string *ptValue, string tDefaultValue)
        : clsValue(strKey, ptValue, tDefaultValue) { }

    virtual ~clsValueString() { }

    void Print()
    {
        string strKey = GetKey();
        string strValue = *GetValuePtr();

        printf("%s %s\n", strKey.c_str(), strValue.c_str());
        CertainLogImpt("%s %s", strKey.c_str(), strValue.c_str());
    }
    int Parse(string strValue)
    {
        *GetValuePtr() = strValue;
        return 0;
    }
};

class clsValueInetAddr : public clsValue<InetAddr_t, INET_ADDR>
{
public:
    clsValueInetAddr() { }

    clsValueInetAddr(string strKey, InetAddr_t *ptValue, InetAddr_t tDefaultValue)
        : clsValue(strKey, ptValue, tDefaultValue) { }

    virtual ~clsValueInetAddr() { }

    void Print()
    {
        string strKey = GetKey();
        InetAddr_t tValue = *GetValuePtr();

        printf("%s %s\n", strKey.c_str(), tValue.ToString().c_str());
        CertainLogImpt("%s %s", strKey.c_str(), tValue.ToString().c_str());
    }
    int Parse(string strInetAddr)
    {
        InetAddr_t *ptInetAddr = GetValuePtr();
        for (size_t i = 0; i < strInetAddr.size(); ++i)
        {
            if (strInetAddr[i] != ':')
            {
                continue;
            }

            AssertLess(i, 16);
            strInetAddr[i] = '\0';
            uint16_t iPort = atoi(strInetAddr.c_str() + i + 1);
            *ptInetAddr = InetAddr_t(strInetAddr.c_str(), iPort);

            return 0;
        }

        return -1;
    }
};

class clsValueInetAddrs : public clsValue<vector<InetAddr_t>, INET_ADDRS>
{
public:
    clsValueInetAddrs() { }

    clsValueInetAddrs(string strKey, vector<InetAddr_t> *ptValue,
            vector<InetAddr_t> tDefaultValue)
        : clsValue(strKey, ptValue, tDefaultValue) { }

    virtual ~clsValueInetAddrs() { }

    void Print()
    {
        string strKey = GetKey();
        vector<InetAddr_t> vecValue = *GetValuePtr();
        for (uint32_t i = 0; i < vecValue.size(); ++i)
        {
            printf("%s[%u] %s\n", strKey.c_str(), i,
                    vecValue[i].ToString().c_str());
            CertainLogImpt("%s[%u] %s", strKey.c_str(), i,
                    vecValue[i].ToString().c_str());
        }
    }
    int Parse(string strInetAddrs)
    {
        int iRet;
        vector<InetAddr_t> *pvecInetAddr = GetValuePtr();
        pvecInetAddr->clear();

        InetAddr_t tInetAddr;
        size_t iPrev = 0, iCurr = 0;

        while (iCurr < strInetAddrs.size())
        {
            if (strInetAddrs[iCurr] == ',')
            {
                string strInetAddr = strInetAddrs.substr(iPrev, iCurr);

                clsValueInetAddr lo("", &tInetAddr, tInetAddr);
                iRet = lo.Parse(strInetAddr);
                if (iRet != 0)
                {
                    fprintf(stderr, "str %s ret %d\n", strInetAddr.c_str(), iRet);
                    return -1;
                }

                pvecInetAddr->push_back(tInetAddr);
                iPrev = iCurr + 1;
            }

            iCurr++;
        }

        if (iPrev < iCurr)
        {
            string strInetAddr = strInetAddrs.substr(iPrev, iCurr);

            clsValueInetAddr lo("", &tInetAddr, tInetAddr);
            iRet = lo.Parse(strInetAddr);
            if (iRet != 0)
            {
                fprintf(stderr, "str %s ret %d\n", strInetAddr.c_str(), iRet);
                return -2;
            }

            pvecInetAddr->push_back(tInetAddr);
        }

        return 0;
    }
};

class clsConfigure
{
public:
    static string s_strConfSuffix;

private:
    string m_strFilePath;

    string m_strCertainPath;
    string m_strPerfLogPath;
    string m_strLogPath;

    map<string, clsValueBase *> m_tKeyMap;

private:
    uint32_t m_iOSSIDKey;
    uint32_t m_iPLogType;
    uint32_t m_iAcceptorNum;
    uint32_t m_iServerNum;
    uint32_t m_iLocalServerID;
    uint32_t m_iReconnIntvMS;
    uint32_t m_iEntityWorkerNum;
    uint32_t m_iIOWorkerNum;
    uint32_t m_iPLogWorkerNum;
    uint32_t m_iDBWorkerNum;
    uint32_t m_iGetAllWorkerNum;
    uint32_t m_iAsyncQueueSize;
    uint32_t m_iIOQueueSize;
    uint32_t m_iPLogQueueSize;
    uint32_t m_iDBQueueSize;
    uint32_t m_iCatchUpQueueSize;
    uint32_t m_iGetAllQueueSize;
    uint32_t m_iIntConnLimit;
    uint32_t m_iEnablePreAuth;
    uint32_t m_iMaxCatchUpSpeedKB;
    uint32_t m_iMaxCatchUpNum;
    uint32_t m_iMaxCatchUpConcurr;
    uint32_t m_iMaxCommitNum;
    uint32_t m_iUseCertainLog;
    uint32_t m_iUseConsole;
    uint32_t m_iLogLevel;
    uint32_t m_iUsePerfLog;
    uint32_t m_iCmdTimeoutMS;
    uint32_t m_iRecoverTimeoutMS;
    uint32_t m_iLocalAcceptFirst;
    uint32_t m_iMaxEntityBitNum;
    uint32_t m_iMaxMemEntityNum;
    uint32_t m_iMaxMemEntryNum;
    uint32_t m_iMaxLeaseMS;
    uint32_t m_iMinLeaseMS;
    uint32_t m_iEnableCheckSum;
    uint32_t m_iDBRoutineCnt;
    uint32_t m_iGetAllRoutineCnt;
    uint32_t m_iDBCtrlPLogGetCnt;
    uint32_t m_iDBCtrlSleepMS;
    uint32_t m_iFlushTimeoutUS;
    uint32_t m_iEnableAutoFixEntry;
    uint32_t m_iGetAllMaxNum;
    uint32_t m_iEnableMaxPLogEntry;
    uint32_t m_iPLogRoutineCnt;
    uint32_t m_iIOReqTimeoutMS;
    uint32_t m_iPLogWriteQueueSize;
    uint32_t m_iPLogWriteWorkerNum;
    uint32_t m_iPLogWriteTimeoutUS;
    uint32_t m_iPLogWriteMaxNum;
    uint32_t m_iUsePLogWriteWorker;
    uint32_t m_iWakeUpTimeoutUS;
    uint32_t m_iEnableTimeStat;
    uint32_t m_iMaxEmbedValueSize;
    uint32_t m_iMaxMultiCmdSizeForC;
    uint32_t m_iMaxMultiCmdStartHour;
    uint32_t m_iMaxMultiCmdHourCnt;
    uint32_t m_iEnableLearnOnly;
    uint32_t m_iMaxCatchUpCnt;
    uint32_t m_iMaxMemCacheSizeMB;
    uint32_t m_iEnableConnectAll; // For compatable
    uint32_t m_iRandomDropRatio;
    uint32_t m_iPLogExpireTimeMS;

    vector<InetAddr_t> m_vecServerAddr;

    int ParseByLine(string strLine, string &strKey, string &strValue);
    int SetByStringValue(clsValueBase *poValue, string strValue);

    int LoadDefaultValue();

    void AddUint32(uint32_t &iUint32, string strKey, uint32_t iDefault);
    void AddString(string &strString, string strKey, string strDefault);
    void AddInetAddr(InetAddr_t &tInetAddr, string strKey,
            InetAddr_t tDefault);
    void AddInetAddrs(vector<InetAddr_t> &vecInetAddrs,
            string strKey, vector<InetAddr_t> vecDefault);

    int AddVariables();
    void RemoveVariables();

    void UpdateConf(clsConfigure *poNewConf);

public:
    clsConfigure(int iArgc, char *pArgv[])
    {
        AssertEqual(AddVariables(), 0);
        AssertEqual(LoadFromOption(iArgc, pArgv), 0);
        AssertEqual(LoadDefaultValue(), 0);
        LoadFromFile(m_strFilePath.c_str());

        // Override configures in the file.
        AssertEqual(LoadFromOption(iArgc, pArgv), 0);
    }

    clsConfigure(const char *pcFilePath)
    {
        assert(pcFilePath != NULL);
        m_strFilePath = pcFilePath;

        AssertEqual(AddVariables(), 0);
        AssertEqual(LoadDefaultValue(), 0);

        LoadFromFile(m_strFilePath.c_str());
    }

    int LoadFromOption(int iArgc, char *pArgv[]);
    int LoadFromFile(const char *pcFilePath = NULL);

    ~clsConfigure()
    {
        RemoveVariables();
    }

    void PrintAll();

public:
    UINT32_GET_SET(OSSIDKey);
    UINT32_GET_SET(PLogType);
    UINT32_GET_SET(AcceptorNum);
    UINT32_GET_SET(LocalServerID);
    UINT32_GET_SET(ServerNum);
    UINT32_GET_SET(ReconnIntvMS);
    UINT32_GET_SET(EntityWorkerNum);
    UINT32_GET_SET(IOWorkerNum);
    UINT32_GET_SET(PLogWorkerNum);
    UINT32_GET_SET(DBWorkerNum);
    UINT32_GET_SET(GetAllWorkerNum);
    UINT32_GET_SET(IOQueueSize);
    UINT32_GET_SET(PLogQueueSize);
    UINT32_GET_SET(DBQueueSize);
    UINT32_GET_SET(CatchUpQueueSize);
    UINT32_GET_SET(GetAllQueueSize);
    UINT32_GET_SET(IntConnLimit);
    UINT32_GET_SET(EnablePreAuth);
    UINT32_GET_SET(MaxCatchUpSpeedKB);
    UINT32_GET_SET(MaxCatchUpNum);
    UINT32_GET_SET(MaxCatchUpConcurr);
    UINT32_GET_SET(MaxCommitNum);
    UINT32_GET_SET(UseCertainLog);
    UINT32_GET_SET(UseConsole);
    UINT32_GET_SET(LogLevel);
    UINT32_GET_SET(UsePerfLog);
    UINT32_GET_SET(CmdTimeoutMS);
    UINT32_GET_SET(RecoverTimeoutMS);
    UINT32_GET_SET(LocalAcceptFirst);
    UINT32_GET_SET(MaxEntityBitNum);
    UINT32_GET_SET(MaxMemEntityNum);
    UINT32_GET_SET(MaxMemEntryNum);
    UINT32_GET_SET(MaxLeaseMS);
    UINT32_GET_SET(MinLeaseMS);
    UINT32_GET_SET(EnableCheckSum);
    UINT32_GET_SET(DBRoutineCnt);
    UINT32_GET_SET(GetAllRoutineCnt);
    UINT32_GET_SET(DBCtrlPLogGetCnt);
    UINT32_GET_SET(DBCtrlSleepMS);
    UINT32_GET_SET(FlushTimeoutUS);
    UINT32_GET_SET(EnableAutoFixEntry);
    UINT32_GET_SET(GetAllMaxNum);
    UINT32_GET_SET(EnableMaxPLogEntry);
    UINT32_GET_SET(PLogRoutineCnt);
    UINT32_GET_SET(IOReqTimeoutMS);
    UINT32_GET_SET(PLogWriteWorkerNum);
    UINT32_GET_SET(PLogWriteQueueSize);
    UINT32_GET_SET(PLogWriteTimeoutUS);
    UINT32_GET_SET(PLogWriteMaxNum);
    UINT32_GET_SET(UsePLogWriteWorker);
    UINT32_GET_SET(WakeUpTimeoutUS);
    UINT32_GET_SET(EnableTimeStat);
    UINT32_GET_SET(MaxEmbedValueSize);
    UINT32_GET_SET(MaxMultiCmdSizeForC);
    UINT32_GET_SET(MaxMultiCmdStartHour);
    UINT32_GET_SET(MaxMultiCmdHourCnt);
    UINT32_GET_SET(EnableLearnOnly);
    UINT32_GET_SET(MaxCatchUpCnt);
    UINT32_GET_SET(MaxMemCacheSizeMB);
    UINT32_GET_SET(EnableConnectAll);
    UINT32_GET_SET(RandomDropRatio);
    UINT32_GET_SET(PLogExpireTimeMS);

    TYPE_GET_SET(string, CertainPath, strCertainPath);
    TYPE_GET_SET(string, PerfLogPath, strPerfLogPath);
    TYPE_GET_SET(string, LogPath, strLogPath);

    //TYPE_GET_SET(vector<InetAddr_t>, ServerAddrs, vecServerAddr);
    const vector<InetAddr_t>& GetServerAddrs()
    {
        return m_vecServerAddr;
    }
    void SetServerAddrs(const vector<InetAddr_t>& vecServerAddr)
    {
        // compatible with gperftools
        m_vecServerAddr.assign(vecServerAddr.begin(), vecServerAddr.end());
    }

    void UpdateServerAddrs(const vector<InetAddr_t>& vecServerAddr)
    {
        assert(vecServerAddr.size() == m_vecServerAddr.size());
        for(int i=0; i<(int)vecServerAddr.size(); i++)
        {
            if(vecServerAddr[i] == m_vecServerAddr[i])
            {
                continue;
            }

            m_vecServerAddr[i] = vecServerAddr[i];
        }
    }

    void LoadAgain();
};

} // namespace Certain

#endif
