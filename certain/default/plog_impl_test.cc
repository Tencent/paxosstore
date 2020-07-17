#include "default/plog_impl.h"

#include "gtest/gtest.h"
#include "utils/log.h"

TEST(PlogImpl, Normal) {
  dbtype::DB* db = nullptr;
  dbtype::Options options;
  options.create_if_missing = true;
  dbtype::Status status = dbtype::DB::Open(options, "test_plog.o", &db);
  ASSERT_TRUE(status.ok());

  PlogImpl plog(db);

  {
    std::string value;
    ASSERT_EQ(plog.GetValue(0, 0, 1, &value), certain::kImplPlogNotFound);

    const std::string mock_value = "Mock Value";
    ASSERT_EQ(plog.SetValue(0, 0, 1, mock_value), 0);

    ASSERT_EQ(plog.GetValue(0, 0, 1, &value), 0);
    ASSERT_EQ(value, mock_value);
  }

  {
    std::string record;
    ASSERT_EQ(plog.GetRecord(0, 0, &record), certain::kImplPlogNotFound);

    const std::string mock_record = "Mock Record";
    ASSERT_EQ(plog.SetRecord(0, 0, mock_record), 0);

    ASSERT_EQ(plog.GetRecord(0, 0, &record), 0);
    ASSERT_EQ(record, mock_record);
  }

  {
    const uint64_t mock_entity = 233;
    const std::string mock_record = "Mock Record";

    ASSERT_EQ(plog.SetRecord(mock_entity - 1, 0, mock_record), 0);
    ASSERT_EQ(plog.SetRecord(mock_entity - 1, 1, mock_record), 0);

    ASSERT_EQ(plog.SetRecord(mock_entity + 0, 0, mock_record), 0);
    ASSERT_EQ(plog.SetRecord(mock_entity + 0, 1, mock_record), 0);
    ASSERT_EQ(plog.SetRecord(mock_entity + 0, 2, mock_record), 0);
    ASSERT_EQ(plog.SetRecord(mock_entity + 0, 3, mock_record), 0);
    ASSERT_EQ(plog.SetValue(mock_entity + 0, 1, 1, ""), 0);

    ASSERT_EQ(plog.SetRecord(mock_entity + 1, 0, mock_record), 0);
    ASSERT_EQ(plog.SetRecord(mock_entity + 1, 1, mock_record), 0);
    ASSERT_EQ(plog.SetRecord(mock_entity + 1, 2, mock_record), 0);

    uint64_t entry = -1;
    ASSERT_EQ(plog.LoadMaxEntry(mock_entity - 2, &entry),
              certain::kImplPlogNotFound);
    ASSERT_EQ(plog.LoadMaxEntry(mock_entity, &entry), 0);
    ASSERT_EQ(entry, 3);

    ASSERT_EQ(plog.LoadMaxEntry(mock_entity + 1, &entry), 0);
    ASSERT_EQ(entry, 2);

    std::vector<std::pair<uint64_t, std::string>> records;
    ASSERT_EQ(plog.RangeGetRecord(mock_entity, 0, 10, &records), 0);
    ASSERT_EQ(records.size(), 3);
    for (auto& record : records) {
      ASSERT_EQ(record.second, mock_record);
    }
  }

  delete db;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_plog.o");
  return RUN_ALL_TESTS();
}
