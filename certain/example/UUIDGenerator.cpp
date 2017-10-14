#include "example/UUIDGenerator.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

#include "utils/Time.h"

const uint64_t clsUUIDGenerator::kUpper = (1LL << 32);

uint32_t GetLocalIP()
{
    char strHostName[128];
    struct hostent *poHostent;

    gethostname(strHostName, sizeof(strHostName));

    poHostent = gethostbyname(strHostName);

    for (int i = 0; poHostent->h_addr_list[i]; i++) {
        in_addr_t tAddr;
        tAddr = inet_addr(inet_ntoa(*(struct in_addr*)(poHostent->h_addr_list[i])));
        return tAddr;
    }
    return 0;
}

void clsUUIDGenerator::Init()
{
    m_iUIP = GetLocalIP();
    m_iCurrTimeUS = Certain::GetCurrTimeUS() % kUpper;
}

uint64_t clsUUIDGenerator::GetUUID()
{
    uint64_t iTime = m_iCount++;
    iTime = (iTime + m_iCurrTimeUS) % kUpper;

    return (iTime << 32) + m_iUIP;
}
