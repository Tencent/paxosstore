
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "Common.h"
#include "Command.h"
#include "network/IOChannel.h"

using namespace Certain;

char g_pcBuffer[IO_BUFFER_SIZE];

clsClientCmd *ReadSingleResult(int iFD)
{
	int iRet = read(iFD, g_pcBuffer, IO_BUFFER_SIZE);
    //printf("read ret %d\n", iRet);
	AssertLess(0, iRet);
	AssertNotMore(RP_HEADER_SIZE, iRet);
	RawPacket_t *lp = (RawPacket_t *)g_pcBuffer;
    ConvertToHostOrder(lp);
	AssertEqual(lp->iLen + RP_HEADER_SIZE, iRet);

	clsCmdFactory *poFactory = clsCmdFactory::GetInstance();
	clsClientCmd *poCmd = dynamic_cast<clsClientCmd *>(
			poFactory->CreateCmd(g_pcBuffer, iRet));
	assert(poCmd != NULL);

	return poCmd;
}

int SerializeToBuffer(clsClientCmd *poCmd)
{
	int iBytes = poCmd->SerializeToArray(g_pcBuffer + RP_HEADER_SIZE,
			IO_BUFFER_SIZE - RP_HEADER_SIZE);
	if (iBytes < 0)
	{
		fprintf(stderr, "SerializeToArray ret %d\n", iBytes);
		return -1;
	}

	RawPacket_t *lp = (RawPacket_t *)g_pcBuffer;
	lp->hMagicNum = htons(RP_MAGIC_NUM);
	lp->hVersion = 0;
	lp->hCmdID = htons(poCmd->GetCmdID());
    lp->hReserve = 0;
	lp->iCheckSum = 0;
	lp->iLen = htonl(iBytes);

	return RP_HEADER_SIZE + iBytes;
}

void SyncWrite(int iFD, clsClientCmd *poCmd)
{
	int iBytes = SerializeToBuffer(poCmd);
	AssertLess(0, iBytes);

	int iRet = write(iFD, g_pcBuffer, iBytes);
    //printf("write ret %d\n", iRet);
	AssertLess(0, iRet);
}

void DoCmd(int iFD, clsClientCmd *poCmd)
{
	uint64_t iUUID = poCmd->GetUUID();
	CertainLogDebug("uuid %lu", iUUID);

	SyncWrite(iFD, poCmd);

	//printf("Begin to ReadSingleResult\n");

	clsClientCmd *poRet= ReadSingleResult(iFD);

	printf("cmd: %s ret %d\n",
			poRet->GetTextCmd().c_str(), poRet->GetResult());

	AssertEqual(poRet->GetUUID(), iUUID);
	AssertEqual(poRet->GetCmdID(), poCmd->GetCmdID());
}

void DoGet(int iFD)
{
	static uint64_t iUUIDGenerator = time(0);

	string strKey;
	cin >> strKey;

	clsSimpleCmd oCmd;
	oCmd.SetSubCmdID(clsSimpleCmd::kGet);

	oCmd.SetKey(strKey);
	oCmd.SetUUID(++iUUIDGenerator);
	AssertEqual(oCmd.GetCmdID(), kSimpleCmd);
	AssertEqual(oCmd.GetSubCmdID(), clsSimpleCmd::kGet);

	DoCmd(iFD, &oCmd);
}

void DoSet(int iFD)
{
	static uint64_t iUUIDGenerator = time(0);

	string strKey, strValue;
	cin >> strKey >> strValue;

	clsSimpleCmd oCmd;
	oCmd.SetSubCmdID(clsSimpleCmd::kSet);

	oCmd.SetKey(strKey);
	oCmd.SetValue(strValue);
	oCmd.SetUUID(++iUUIDGenerator);

	AssertEqual(oCmd.GetCmdID(), kSimpleCmd);
	AssertEqual(oCmd.GetSubCmdID(), clsSimpleCmd::kSet);

	DoCmd(iFD, &oCmd);
}

int main(int argc, char **argv)
{
	if (argc != 3)
	{
		fprintf(stderr, "%s srv_IP srv_Port\n", argv[0]);
		exit(-1);
	}

	assert(strlen(argv[1]) < 16);

	InetAddr_t tSrvEndpoint(argv[1], strtoull(argv[2], 0, 10));

	int iFD = CreateSocketFD(NULL);
	if (iFD < 0)
	{
		CertainLogError("CreateSocketFD ret %d", iFD);
		exit(-1);
	}

	int iRet = Connect(iFD, tSrvEndpoint);
	if (iRet != 0)
	{
		CertainLogError("Connect ret %d", iRet);
		exit(-1);
	}

	string strCmd;
	while (cin >> strCmd)
	{
		//uint64_t iStart = GetCurrTimeMS();

		if (strCmd == "Get")
		{
			DoGet(iFD);
		}
		else if (strCmd == "Set")
		{
			DoSet(iFD);
		}
		else
		{
			fprintf(stderr, "Unknown cmd!\n");
			exit(-1);
		}

		//uint64_t iEnd = GetCurrTimeMS();

		//printf(" use_time %lu\n", iEnd - iStart);
	}

	return 0;
}
