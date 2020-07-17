#include "default/db_impl.h"

#include "certain/errors.h"
#include "gtest/gtest.h"
#include "utils/log.h"

TEST(DbImpl, Normal) {
  DbImpl db;
  const std::string value(100, 'A');

  const uint64_t mock_entity = 233;
  ASSERT_EQ(db.Commit(mock_entity, 1, value), 0);
  ASSERT_EQ(db.Commit(mock_entity, 2, value), 0);
  ASSERT_EQ(db.Commit(mock_entity, 3, value), 0);

  uint64_t max_committed_entry;
  certain::Db::RecoverFlag flag;
  ASSERT_EQ(db.GetStatus(0, &max_committed_entry, &flag),
            certain::kImplDbNotFound);
  ASSERT_EQ(db.GetStatus(mock_entity, &max_committed_entry, &flag), 0);
  ASSERT_EQ(max_committed_entry, 3);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
