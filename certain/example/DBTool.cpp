#include <string>

#include "Certain.pb.h"

#include "example/Coding.h"
#include "example/example.pb.h"

void Run(const std::string& strName) 
{
    dbtype::Status s;

    // Does not exist, and create_if_missing == false: error
    dbtype::DB* poLevelDB = NULL;
    dbtype::Options tOpt;
    tOpt.create_if_missing = true;
    assert(dbtype::DB::Open(tOpt, strName, &poLevelDB).ok());

    {
        dbtype::ReadOptions tRopt;
        std::unique_ptr<dbtype::Iterator> iter(poLevelDB->NewIterator(tRopt));
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            const dbtype::Slice oKey = iter->key();
            const std::string strValue(iter->value().data(), iter->value().size());

            uint64_t iEntityID = 0, iInfoID = 0;
            uint64_t iEntry = 0, iValueID = 0;
            if (DecodeInfoKey(oKey, iEntityID, iInfoID))
            {
                example::CardInfo tCardInfo;
                assert(tCardInfo.ParseFromString(strValue));
                printf("infokv:\t iEntityID=%lu iInfoID=%lu user_name=%s user_id=%lu balance=%lu\n", 
                        iEntityID, iInfoID, tCardInfo.user_name().c_str(), tCardInfo.user_id(), 
                        tCardInfo.balance());
            }
            else if (DecodeEntityMetaKey(oKey, iEntityID))
            {
                CertainPB::DBEntityMeta meta;
                assert(meta.ParseFromString(strValue));
                printf("metakv:\t entity_id=%lu flag=%u commited_entry=%lu\n", 
                        iEntityID, meta.flag(), meta.max_commited_entry());
            }
            else if (DecodePLogKey(oKey, iEntityID, iEntry))
            {
                printf("plogkv:\t entity_id=%lu entry=%lu size=%ld\n",
                        iEntityID, iEntry, strValue.size());
            }
            else if (DecodePLogValueKey(oKey, iEntityID, iEntry, iValueID))
            {
                printf("plogkv:\t entity_id=%lu entry=%lu value_id=%lu size=%ld\n", 
                        iEntityID, iEntry, iValueID, strValue.size());
            }
            else if (DecodePLogMetaKey(oKey, iEntityID))
            {
                CertainPB::PLogEntityMeta meta;
                assert(meta.ParseFromString(strValue));
                printf("plogkv:\t entity_id=%lu max_plog_entry=%lu\n", 
                        iEntityID, meta.max_plog_entry());
            }
            else
            {
                assert(false);
            }
        }
    }

    printf("Finish\n");

    delete poLevelDB, poLevelDB = NULL;
}

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        printf("usage: %s dbname\n", argv[0]);
        return -1;
    }

    Run(argv[1]);

    return 0;
}

