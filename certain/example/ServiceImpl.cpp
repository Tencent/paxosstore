#include "ServiceImpl.h"

using namespace Certain;

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
    uint64_t iEntityID = oRequest.card_id();

    clsAutoEntityLock oEntityLock(iEntityID);

    uint64_t iEntry = 0, iMaxCommitedEntry = 0;
    clsCertainWrapper* poCertain = clsCertainWrapper::GetInstance();
    int iRet = poCertain->EntityCatchUp(iEntityID, iMaxCommitedEntry);
    if (iRet != 0) return iRet;

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    string strKey;
    EncodeInfoKey(strKey, iEntityID, oRequest.card_id());

    string strValue;
    iRet = poDBEngine->Get(strKey, strValue);
    if (iRet != 0 && iRet != eRetCodeNotFound)
    {
        return iRet;
    }
    if (iRet == 0)
    {
        return example::StatusCode::eCardExist;
    }
    
    strValue.clear();
    assert(oRequest.card_info().SerializeToString(&strValue));

    string strWriteBatch;
    iRet = poDBEngine->Put(strKey, strValue, &strWriteBatch);
    if (iRet != 0)
    {
        return iRet;
    }

    iEntry = iMaxCommitedEntry + 1;
    std::vector<uint64_t> vecUUID;
    if (oRequest.uuid() != 0)
    {
        vecUUID.push_back(oRequest.uuid());
    }
    iRet = poCertain->RunPaxos(iEntityID, iEntry, example::OperCode::eInsertCard, vecUUID, strWriteBatch);
    if (iRet != 0)
    {
        return iRet;
    }

    return 0;
}

int clsServiceImpl::UpdateCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    uint64_t iEntityID = oRequest.card_id();

    clsAutoEntityLock oEntityLock(iEntityID);

    uint64_t iEntry = 0, iMaxCommitedEntry = 0;
    clsCertainWrapper* poCertain = clsCertainWrapper::GetInstance();
    int iRet = poCertain->EntityCatchUp(iEntityID, iMaxCommitedEntry);
    if (iRet != 0) return iRet;
    
    string strKey;
    EncodeInfoKey(strKey, iEntityID, oRequest.card_id());

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    string strValue;
    iRet = poDBEngine->Get(strKey, strValue);
    if (iRet == eRetCodeNotFound) 
    {
        return example::StatusCode::eCardNotExist;
    }
    else if (iRet != 0)
    {
        return iRet;
    }
    if (!oResponse.mutable_card_info()->ParseFromString(strValue)) {
        return eRetCodeParseProtoErr;
    }

    if ((oRequest.delta() < 0) && (oResponse.card_info().balance() < -(uint64_t)oRequest.delta()))
    {
        return example::StatusCode::eCardBalanceNotEnough;
    }
    oResponse.mutable_card_info()->set_last_modified_time(time(0));
    oResponse.mutable_card_info()->set_balance(oResponse.card_info().balance() + oRequest.delta());
    strValue.clear();
    assert(oResponse.card_info().SerializeToString(&strValue));

    string strWriteBatch;
    iRet = poDBEngine->Put(strKey, strValue, &strWriteBatch);
    if (iRet != 0)
    {
        return iRet;
    }

    iEntry = iMaxCommitedEntry + 1;
    std::vector<uint64_t> vecUUID;
    if (oRequest.uuid() != 0)
    {
        vecUUID.push_back(oRequest.uuid());
    }
    iRet = poCertain->RunPaxos(iEntityID, iEntry, example::OperCode::eUpdateCard, vecUUID, strWriteBatch);
    if (iRet != 0)
    {
        return iRet;
    }

    return 0;
}

int clsServiceImpl::DeleteCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    uint64_t iEntityID = oRequest.card_id();

    clsAutoEntityLock oEntityLock(iEntityID);

    uint64_t iEntry = 0, iMaxCommitedEntry = 0;
    clsCertainWrapper* poCertain = clsCertainWrapper::GetInstance();
    int iRet = poCertain->EntityCatchUp(iEntityID, iMaxCommitedEntry);
    if (iRet != 0) return iRet;
    
    string strKey;
    EncodeInfoKey(strKey, iEntityID, oRequest.card_id());

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    string strValue;
    iRet = poDBEngine->Get(strKey, strValue);
    if (iRet == eRetCodeNotFound) 
    {
        return example::StatusCode::eCardNotExist;
    }
    else if (iRet != 0)
    {
        return iRet;
    }

    string strWriteBatch;
    iRet = poDBEngine->Delete(strKey, &strWriteBatch);
    if (iRet != 0)
    {
        return iRet;
    }

    iEntry = iMaxCommitedEntry + 1;
    std::vector<uint64_t> vecUUID;
    if (oRequest.uuid() != 0)
    {
        vecUUID.push_back(oRequest.uuid());
    }
    iRet = poCertain->RunPaxos(iEntityID, iEntry, example::OperCode::eDeleteCard, vecUUID, strWriteBatch);
    if (iRet != 0)
    {
        return iRet;
    }

    return 0;
}

// Read command
int clsServiceImpl::SelectCard(grpc::ServerContext& oContext,
        const example::CardRequest& oRequest,
        example::CardResponse& oResponse)
{
    uint64_t iEntityID = oRequest.card_id();

    clsAutoEntityLock oEntityLock(iEntityID);

    uint64_t iEntry = 0, iMaxCommitedEntry = 0;
    clsCertainWrapper* poCertain = clsCertainWrapper::GetInstance();
    int iRet = poCertain->EntityCatchUp(iEntityID, iMaxCommitedEntry);
    if (iRet != 0) return iRet;
    
    iEntry = iMaxCommitedEntry + 1;
    static const string strWriteBatch;
    static const std::vector<uint64_t> vecUUID;
    iRet = poCertain->RunPaxos(iEntityID, iEntry, example::OperCode::eSelectCard, vecUUID, strWriteBatch);
    if (iRet != 0) return iRet;

    string strKey;
    EncodeInfoKey(strKey, iEntityID, oRequest.card_id());

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    string strValue;
    iRet = poDBEngine->Get(strKey, strValue); 
    if (iRet == eRetCodeNotFound)
    {
        return example::StatusCode::eCardNotExist;
    }
    else if (iRet != 0)
    {
        return iRet;
    }

    if (!oResponse.mutable_card_info()->ParseFromString(strValue)) {
        return eRetCodeParseProtoErr;
    }

    return 0;
}

// GetAllAndSet
int clsServiceImpl::GetDBEntityMeta(grpc::ServerContext& oContext,
        const ::example::GetRequest& oRequest,
        example::GetResponse& oResponse)
{
    uint64_t iEntityID = oRequest.entity_id();

    int iRet = 0;
    uint64_t iEntry = 0, iMaxCommitedEntry = 0;

    clsAutoEntityLock oEntityLock(iEntityID);

    clsCertainWrapper* poCertain = clsCertainWrapper::GetInstance();
    iRet = poCertain->EntityCatchUp(iEntityID, iMaxCommitedEntry);
    if (iRet != 0) return iRet;

    static const string strWriteBatch;
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
    if (iFlag != 0) return eRetCodeGetDBEntityMetaErr;
    
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
        return eRetCodeRejectAll;

    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

    const dbtype::Snapshot* poSnapshot = NULL;
    int iRet = poDBEngine->FindSnapshot(oRequest.sequence_number(), poSnapshot);
    if (iRet != 0) return iRet;

    string strNextKey = oRequest.next_key();
    string strWriteBatch;
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
        return oRetCodeLocalNotRejectAll;
    
    uint64_t iEntityID = oRequest.entity_id();

    clsAutoEntityLock oEntityLock(iEntityID);

    uint64_t iMaxCommitedEntry = 0;
    uint32_t iFlag = 0;
    clsDBImpl* poDBEngine = dynamic_cast<clsDBImpl*>(
            Certain::clsCertainWrapper::GetInstance()->GetDBEngine());

	poDBEngine->GetEntityMeta(iEntityID, iMaxCommitedEntry, iFlag);
    if (iMaxCommitedEntry > 0 && iFlag == 0) return 0;

    return poDBEngine->GetAllAndSet(iEntityID, 0, iMaxCommitedEntry);
}
