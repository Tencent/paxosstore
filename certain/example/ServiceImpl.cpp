#include "ServiceImpl.h"

int clsServiceImpl::Echo(grpc::ServerContext& oContext,
        const example::EchoRequest& oRequest,
        example::EchoResponse& oResponse)
{
    // Other interfaces: EntityCatchUp and RunPaxos here.
    oResponse.set_value(oRequest.value());
    return 0;
}

// Write command
int clsServiceImpl::InsertCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    return BatchFunc(example::OperCode::eInsertCard, oRequest, oResponse);
}

int clsServiceImpl::UpdateCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    return BatchFunc(example::OperCode::eUpdateCard, oRequest, oResponse);
}

int clsServiceImpl::DeleteCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    return BatchFunc(example::OperCode::eDeleteCard, oRequest, oResponse);
}

// Read command
int clsServiceImpl::SelectCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    int iRet = BatchFunc(example::OperCode::eSelectCard, oRequest, oResponse);
    if (iRet != 0) return iRet;

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());
    dbtype::DB *poDB = poDBEngine->GetDB();
    clsTemporaryTable oTable(poDB);

    std::string strKey;
    EncodeInfoKey(strKey, oRequest.entity_id(), oRequest.card_id());
    std::string strValue;
    iRet = oTable.Get(strKey, strValue);
    if (iRet == Certain::eRetCodeNotFound)
    {
        return example::StatusCode::eCardNotExist;
    }

    if (iRet == 0 && !oResponse.mutable_card_info()->ParseFromString(strValue)) {
        return Certain::eRetCodeParseProtoErr;
    }

    return iRet;
}

// GetAllAndSet
int clsServiceImpl::GetDBEntityMeta(grpc::ServerContext& oContext,
        const ::example::GetRequest& oRequest,
        example::GetResponse& oResponse)
{
    uint64_t iEntityID = oRequest.entity_id();

    int iRet = 0;
    uint64_t iEntry = 0, iMaxCommitedEntry = 0;

    Certain::clsAutoEntityLock oEntityLock(iEntityID);

    Certain::clsCertainWrapper* poCertain = Certain::clsCertainWrapper::GetInstance();
    iRet = poCertain->EntityCatchUp(iEntityID, iMaxCommitedEntry);
    if (iRet != 0) return iRet;

    static const std::string strWriteBatch;
    static const std::vector<uint64_t> vecUUID;
    iEntry = iMaxCommitedEntry + 1;
    iRet = poCertain->RunPaxos(iEntityID, iEntry, example::OperCode::eGetDBEntityMeta, vecUUID, strWriteBatch);
    if (iRet != 0) return iRet;

	uint32_t iFlag = 0;
    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    uint64_t iSequenceNumber = 0;
    const dbtype::Snapshot* poSnapshot = NULL;
    iRet = poDBEngine->InsertSnapshot(iSequenceNumber, poSnapshot);
    if (iRet != 0) return iRet;

	iRet = poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag, poSnapshot);

    if (iRet != 0) return iRet;
    if (iFlag != 0) return Certain::eRetCodeGetDBEntityMetaErr;
    
    oResponse.set_flag(iFlag);
    oResponse.set_max_commited_entry(iMaxCommitedEntry);
    oResponse.set_sequence_number(iSequenceNumber);
    
    return 0;
}

int clsServiceImpl::GetAllForCertain(grpc::ServerContext& oContext,
        const example::GetRequest& oRequest,
        example::GetResponse& oResponse)
{ 
    if (Certain::clsCertainWrapper::GetInstance()->GetConf()->GetEnableLearnOnly())
        return Certain::eRetCodeRejectAll;

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    const dbtype::Snapshot* poSnapshot = NULL;
    int iRet = poDBEngine->FindSnapshot(oRequest.sequence_number(), poSnapshot);
    if (iRet != 0) return iRet;

    std::string strNextKey = oRequest.next_key();
    std::string strWriteBatch;
    iRet = poDBEngine->GetBatch(oRequest.entity_id(), strNextKey, &strWriteBatch, 
            poSnapshot, oRequest.max_size());
    if (iRet != 0) return iRet;

    oResponse.set_next_key(strNextKey);
    oResponse.set_value(strWriteBatch);

    return 0;
}

int clsServiceImpl::RecoverData(grpc::ServerContext& oContext,
        const example::GetRequest& oRequest,
        example::GetResponse& oResponse)
{
    if (!Certain::clsCertainWrapper::GetInstance()->GetConf()->GetEnableLearnOnly())
        return Certain::oRetCodeLocalNotRejectAll;
    
    uint64_t iEntityID = oRequest.entity_id();

    uint64_t iMaxCommitedEntry = 0;
    uint32_t iFlag = 0;
    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    {
        Certain::clsAutoEntityLock oEntityLock(iEntityID);
	    poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
    }
    if (iMaxCommitedEntry > 0 && iFlag == 0) return 0;

    return poDBEngine->GetAllAndSet(iEntityID, 0, iMaxCommitedEntry);
}

int clsServiceImpl::BatchFunc(int iOper,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    uint64_t iStartUS = Certain::GetCurrTimeUS();

    // 1. Push in queue
    uint64_t iPushStartUS = Certain::GetCurrTimeUS();
    QueueItem_t *poItem = new QueueItem_t;
    Certain::clsAutoDelete<QueueItem_t> oAutoDelete(poItem);
    poItem->iOper = iOper;
    poItem->iEntityID = oRequest.entity_id();
    poItem->poRequest = (void*)&oRequest;
    poItem->poResponse = (void*)&oResponse;
    poItem->iRet = BatchStatus::WAITING;

    {
        Certain::clsThreadLock oLock(&m_poBatchMapMutex);
        m_oBatchMap[poItem->iEntityID].push(poItem);
    }
    uint64_t iPushEndUS = Certain::GetCurrTimeUS();

    // 2. Lock
    uint64_t iLockStartUS = Certain::GetCurrTimeUS();
    Certain::clsAutoEntityLock oAuto(poItem->iEntityID);

    if (poItem->iRet != BatchStatus::WAITING)
    {
        return poItem->iRet;
    }

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());
    dbtype::DB *poDB = poDBEngine->GetDB();
    clsTemporaryTable oTable(poDB);
    uint64_t iLockEndUS = Certain::GetCurrTimeUS();

    // 3. Pop requests with the same entity.
    uint64_t iPopStartUS = Certain::GetCurrTimeUS();
    std::queue<QueueItem_t*> oQueue;
    {
        Certain::clsThreadLock oLock(&m_poBatchMapMutex);
        auto iter = m_oBatchMap.find(poItem->iEntityID);
        assert(iter != m_oBatchMap.end());
        while (!iter->second.empty())
        {
            oQueue.push(iter->second.front());
            iter->second.pop();
        }
    }
    uint64_t iPopEndUS = Certain::GetCurrTimeUS();

    // 4. EntityCatchUp
    uint64_t iCatchUpStartUS = Certain::GetCurrTimeUS();
    Certain::clsCertainWrapper* poCertain = Certain::clsCertainWrapper::GetInstance();
    uint64_t iEntry = 0, iMaxCommitedEntry = 0;
    int iRet = poCertain->EntityCatchUp(poItem->iEntityID, iMaxCommitedEntry);
    if (iRet != 0) 
    {
        BatchReturn(&oQueue, iRet);
        return iRet;
    }
    uint64_t iCatchUpEndUS = Certain::GetCurrTimeUS();

    // 5. clsTemporaryTable: Handle requests.
    uint64_t iBatchHandleStartUS = Certain::GetCurrTimeUS();
    std::vector<uint64_t> vecUUID;
    uint64_t iQueueSize = oQueue.size();
    uint64_t iRead = 0, iWrite = 0;
    while (iQueueSize > 0)
    {
        QueueItem_t* poItem = oQueue.front();
        oQueue.pop();
        --iQueueSize;

        assert(poItem->iRet == BatchStatus::WAITING);
        HandleSingleCommand(&oTable, poItem, &vecUUID);
        if (poItem->iOper == example::OperCode::eSelectCard) iRead++; else iWrite++;
        // Re-push items since they need to RunPaxos.
        oQueue.push(poItem);
    }
    uint64_t iBatchHandleEndUS = Certain::GetCurrTimeUS();

    // 6. RunPaxos
    uint64_t iRunPaxosStartUS = Certain::GetCurrTimeUS();
    iEntry = iMaxCommitedEntry + 1;
    iRet = poCertain->RunPaxos(poItem->iEntityID, iEntry, example::OperCode::eBatchFunc, vecUUID, 
            oTable.GetWriteBatchString());
    BatchReturn(&oQueue, iRet);
    uint64_t iRunPaxosEndUS = Certain::GetCurrTimeUS();

    uint64_t iEndUS = Certain::GetCurrTimeUS();

    CertainLogError("iEntityID %lu iPushTime %lu iLockTime %lu iPopTime %lu "
            "iCatchUpTime %lu iBatchHandleTime %lu iRunPaxos %lu iTotalUS %lu "
            "iRead %lu iWrite %lu",
            poItem->iEntityID, 
            iPushEndUS - iPushStartUS, iLockEndUS - iLockStartUS, iPopEndUS - iPopStartUS, 
            iCatchUpEndUS - iCatchUpStartUS, iBatchHandleEndUS - iBatchHandleStartUS, 
            iRunPaxosEndUS - iRunPaxosStartUS, iEndUS - iStartUS, iRead, iWrite);

    iRet = poItem->iRet;
    return iRet;
}

void clsServiceImpl::BatchReturn(std::queue<QueueItem_t*>* poQueue, int iRet)
{
    QueueItem_t* poItem = NULL;
    while (!poQueue->empty())
    {
        poItem = poQueue->front();
        poQueue->pop();
        if (poItem->iRet == BatchStatus::WAITING)
            poItem->iRet = iRet;
    }
}

void clsServiceImpl::HandleSingleCommand(clsTemporaryTable* poTable, QueueItem_t* poItem, 
        std::vector<uint64_t>* poVecUUID)
{
    const example::CardRequest *poRequest = (const example::CardRequest *)poItem->poRequest;
    example::CardResponse *poResponse = (example::CardResponse *)poItem->poResponse;

    std::string strKey;
    EncodeInfoKey(strKey, poItem->iEntityID, poRequest->card_id());

    std::string strValue;
    int iRet = poTable->Get(strKey, strValue);

    switch (poItem->iOper)
    {
        case example::OperCode::eInsertCard:
            if (iRet != Certain::eRetCodeOK && iRet != Certain::eRetCodeNotFound)
            {
                poItem->iRet = iRet;
                return;
            }
            else if (iRet == Certain::eRetCodeOK) 
            {
                poItem->iRet = example::StatusCode::eCardExist;
                return;
            }

            strValue.clear();
            assert(poRequest->card_info().SerializeToString(&strValue));
            iRet = poTable->Put(strKey, strValue);
            break;
        case example::OperCode::eUpdateCard:
            if (iRet == Certain::eRetCodeNotFound) 
            {
                poItem->iRet = example::StatusCode::eCardNotExist;
                return;
            }
            else if (iRet != Certain::eRetCodeOK)
            {
                poItem->iRet = iRet;
                return;
            }

            if (!poResponse->mutable_card_info()->ParseFromString(strValue))
            {
                poItem->iRet = Certain::eRetCodeParseProtoErr;
                return;
            }
            if ((poRequest->delta() < 0) && (poResponse->card_info().balance() < -(uint64_t)poRequest->delta()))
            {
                poItem->iRet = example::StatusCode::eCardBalanceNotEnough;
                return;
            }

            poResponse->mutable_card_info()->set_last_modified_time(time(0));
            poResponse->mutable_card_info()->set_balance(poResponse->card_info().balance() + poRequest->delta());

            strValue.clear();
            assert(poResponse->card_info().SerializeToString(&strValue));
            iRet = poTable->Put(strKey, strValue);
            break;
        case example::OperCode::eDeleteCard:
            if (iRet == Certain::eRetCodeNotFound) 
            {
                poItem->iRet = example::StatusCode::eCardNotExist;
                return;
            }
            else if (iRet != Certain::eRetCodeOK)
            {
                poItem->iRet = iRet;
                return;
            }

            iRet = poTable->Delete(strKey);
            break;
        default:  // Read command, don't do anything
            return;
    }

    if (iRet == Certain::eRetCodeOK)
    {
        // Don't set poItem->iRet if iRet == 0, since it should be determined by the result of RunPaxos.
        if (poRequest->uuid() != 0) poVecUUID->push_back(poRequest->uuid());
    }
    else 
    {
        poItem->iRet = iRet;
    }
}
