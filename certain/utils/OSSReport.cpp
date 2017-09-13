#include "utils/OSSReport.h"

uint32_t s_iCertainOSSIDKey = 0;

namespace Certain
{

namespace OSS
{

void SetCertainOSSIDKey(uint32_t iOSSIDKey)
{
    s_iCertainOSSIDKey = iOSSIDKey;
}

}; // namespace OSS

}; // namespace Certain
