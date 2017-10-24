#include "IOChannel.h"

namespace Certain
{

int clsIOChannel::Read(char *pcBuffer, uint32_t iSize)
{
    int iRet;
    int iFD = clsFDBase::GetFD();

    if (m_bBroken)
    {
        return -1;
    }

    Assert(clsFDBase::IsReadable());

    uint32_t iCurr = m_strReadBytes.size();
    AssertLess(iCurr, iSize);

    memcpy(pcBuffer, m_strReadBytes.c_str(), iCurr);
    m_strReadBytes.clear();

    while (iCurr < iSize)
    {
        iRet = read(iFD, pcBuffer + iCurr, iSize - iCurr);

        if (iRet > 0)
        {
            iCurr += iRet;
            m_iTotalReadCnt += iRet;
        }
        else if (iRet == 0)
        {
            InetAddr_t tPeerAddr = m_tConnInfo.tPeerAddr;
            CertainLogError("closed by peer %s fd %d",
                    tPeerAddr.ToString().c_str(), iFD);

            m_bBroken = true;
        }
        else if (iRet == -1)
        {
            CertainLogDebug("read ret -1 fd %d errno %d", iFD, errno);

            if (errno == EINTR)
            {
                continue;
            }
            if (errno == EAGAIN)
            {
                clsFDBase::SetReadable(false);
                break;
            }

            m_bBroken = true;

            CertainLogError("read ret -1 conn: %s errno %d",
                    m_tConnInfo.ToString().c_str(), errno);
        }

        if (m_bBroken)
        {
            break;
        }
    }

    if (iCurr == 0)
    {
        Assert(m_bBroken);
        return -2;
    }

    return iCurr;
}

int clsIOChannel::Write(const char *pcBuffer, uint32_t iSize)
{
    int iRet;

    iRet = AppendWriteBytes(pcBuffer, iSize);
    if (iRet != 0)
    {
        CertainLogError("AppendWriteBytes ret %d", iRet);
        return -1;
    }

    iRet = FlushWriteBuffer();
    if (iRet != 0)
    {
        CertainLogError("FlushWriteBuffer ret %d", iRet);
        return -2;
    }

    return iSize;
}

int clsIOChannel::FlushWriteBuffer()
{
    int iRet;

    if (m_bBroken)
    {
        return -1;
    }
    Assert(clsFDBase::IsWritable());

    uint32_t iDataSize = GetSize(&m_tWriteBuffer);
    if (iDataSize == 0)
    {
        return 0;
    }

    int iFD = clsFDBase::GetFD();
    struct iovec atBuffer[2] = { 0 };

    atBuffer[0].iov_base = m_tWriteBuffer.pcData + m_tWriteBuffer.iStart1;
    atBuffer[0].iov_len = m_tWriteBuffer.iEnd1 - m_tWriteBuffer.iStart1;

    atBuffer[1].iov_base = m_tWriteBuffer.pcData + m_tWriteBuffer.iStart0;
    atBuffer[1].iov_len = m_tWriteBuffer.iEnd0 - m_tWriteBuffer.iStart0;

    int iBufferIdx = 0;

    while (iDataSize > 0)
    {
        AssertLess(iBufferIdx, 2);
        iRet = writev(iFD, atBuffer + iBufferIdx, 2 - iBufferIdx);
        AssertNotEqual(iRet, 0);

        if (iRet > 0)
        {
            m_iTotalWriteCnt += iRet;
            iDataSize -= iRet;

            while (iRet > 0)
            {
                AssertLess(iBufferIdx, 2);

                if (size_t(iRet) < atBuffer[iBufferIdx].iov_len)
                {
                    atBuffer[iBufferIdx].iov_len -= iRet;
                    atBuffer[iBufferIdx].iov_base = (
                            char *)atBuffer[iBufferIdx].iov_base + iRet;
                    iRet = 0;
                }
                else
                {
                    iRet -= atBuffer[iBufferIdx].iov_len;
                    atBuffer[iBufferIdx].iov_len = 0;
                    iBufferIdx++;
                }
            }
        }
        else if (iRet == -1)
        {
            CertainLogDebug("read ret -1 fd %d errno %d", iFD, errno);

            if (errno == EINTR)
            {
                continue;
            }
            if (errno == EAGAIN)
            {
                clsFDBase::SetWritable(false);
                break;
            }

            m_bBroken = true;
            CertainLogError("write fail fd %d errno %d", iFD, errno);
            break;
        }
    }

    m_tWriteBuffer.iStart0 = m_tWriteBuffer.iEnd0 - atBuffer[1].iov_len;
    m_tWriteBuffer.iStart1 = m_tWriteBuffer.iEnd1 - atBuffer[0].iov_len;
    UpdateBuffer(&m_tWriteBuffer);

    if (m_bBroken)
    {
        return -2;
    }

    return 0;
}

int clsIOChannel::AppendReadBytes(const char *pcBuffer, uint32_t iSize)
{
    m_strReadBytes.append(pcBuffer, iSize);
    return 0;
}

int clsIOChannel::AppendWriteBytes(const char *pcBuffer, uint32_t iSize)
{
    UpdateBuffer(&m_tWriteBuffer);

    if (iSize > m_tWriteBuffer.iStart1 - m_tWriteBuffer.iEnd0)
    {
        return -1;
    }

    memcpy(m_tWriteBuffer.pcData + m_tWriteBuffer.iEnd0, pcBuffer, iSize);
    m_tWriteBuffer.iEnd0 += iSize;

    return 0;
}

int clsIOChannel::AppendWriteBytes(clsSerializeCBBase *poCB)
{
    UpdateBuffer(&m_tWriteBuffer);

    int iRet = poCB->Call(m_tWriteBuffer.pcData + m_tWriteBuffer.iEnd0,
            m_tWriteBuffer.iStart1 - m_tWriteBuffer.iEnd0);
    if (iRet < 0)
    {
        CertainLogError("poCB->Call max_size %u ret %d",
                m_tWriteBuffer.iStart1 - m_tWriteBuffer.iEnd0, iRet);
        return -1;
    }
    m_tWriteBuffer.iEnd0 += iRet;

    return 0;
}

void clsIOChannel::PrintInfo()
{
    CertainLogError("conn %s total_read %lu total_write %lu "
            "read_size %lu write_size %u",
            m_tConnInfo.ToString().c_str(), m_iTotalReadCnt,
            m_iTotalWriteCnt, m_strReadBytes.size(),
            GetSize(&m_tWriteBuffer));

    m_iTotalReadCnt = 0;
    m_iTotalWriteCnt = 0;
}

} // namespace Certain
