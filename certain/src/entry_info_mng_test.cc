#include "src/entry_info_mng.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"

TEST(EntryInfoMng, Normal) {
  certain::Options options;
  certain::EntryInfoMng mng(&options, 0);
  certain::EntityInfoMng entity_mng(&options);

  uint64_t entity_1 = 1;
  uint64_t entity_2 = 2;
  certain::EntityInfo* entity_info_1 =
      entity_mng.CreateEntityInfo(entity_1, 2, 3);
  certain::EntityInfo* entity_info_2 =
      entity_mng.CreateEntityInfo(entity_2, 3, 4);

  ASSERT_EQ(mng.FindEntryInfo(1, 10), nullptr);
  ASSERT_EQ(mng.FindEntryInfo(2, 20), nullptr);
  ASSERT_NE(mng.CreateEntryInfo(entity_info_2, 20), nullptr);
  ASSERT_EQ(mng.FindEntryInfo(1, 10), nullptr);
  ASSERT_NE(mng.FindEntryInfo(2, 20), nullptr);
  ASSERT_NE(mng.CreateEntryInfo(entity_info_1, 10), nullptr);
  ASSERT_NE(mng.FindEntryInfo(1, 10), nullptr);
  certain::EntryInfo* entry_info = nullptr;

  entry_info = mng.FindEntryInfo(1, 10);
  ASSERT_EQ(entry_info->entry, 10);
  ASSERT_EQ(entry_info->entity_info->entity_id, 1);
  entry_info = mng.FindEntryInfo(2, 20);
  ASSERT_EQ(entry_info->entry, 20);
  ASSERT_EQ(entry_info->entity_info->entity_id, 2);

  mng.DestroyEntryInfo(entry_info);
  ASSERT_EQ(mng.FindEntryInfo(2, 20), nullptr);
}

TEST(EntryInfoMng, MakeEnoughRoom) {
  certain::Options options;
  options.set_max_mem_entry_num(1);
  options.set_entity_worker_num(1);
  certain::EntryInfoMng mng(&options, 0);
  certain::EntityInfoMng entity_mng(&options);

  uint64_t entity_1 = 1;
  certain::EntityInfo* entity_info_1 =
      entity_mng.CreateEntityInfo(entity_1, 2, 3);

  mng.CreateEntryInfo(entity_info_1, 1);
  auto info_2 = mng.CreateEntryInfo(entity_info_1, 2);
  mng.CreateEntryInfo(entity_info_1, 3);
  auto info_4 = mng.CreateEntryInfo(entity_info_1, 4);

  mng.MoveToOldChosenList(info_2);
  mng.MoveToOldChosenList(info_4);
  ASSERT_TRUE(mng.MakeEnoughRoom());
  ASSERT_EQ(mng.FindEntryInfo(entity_1, 1), nullptr);
  ASSERT_EQ(mng.FindEntryInfo(entity_1, 2), nullptr);
  ASSERT_NE(mng.FindEntryInfo(entity_1, 3), nullptr);
  ASSERT_EQ(mng.FindEntryInfo(entity_1, 4), nullptr);
}

TEST(EntryInfoMng, CleanupExpiredChosenEntry) {
  certain::Options options;
  options.set_entry_timeout_sec(1);
  certain::EntryInfoMng mng(&options, 0);
  certain::EntityInfoMng entity_mng(&options);

  uint64_t entity_1 = 1;
  certain::EntityInfo* entity_info_1 =
      entity_mng.CreateEntityInfo(entity_1, 2, 3);

  auto info_1 = mng.CreateEntryInfo(entity_info_1, 20);
  auto info_2 = mng.CreateEntryInfo(entity_info_1, 21);

  mng.MoveToOldChosenList(info_1);
  mng.CleanupExpiredChosenEntry();
  ASSERT_NE(mng.FindEntryInfo(entity_1, 20), nullptr);
  usleep(1000 * 1000);
  mng.MoveToOldChosenList(info_2);
  mng.CleanupExpiredChosenEntry();
  ASSERT_EQ(mng.FindEntryInfo(entity_1, 20), nullptr);
  ASSERT_NE(mng.FindEntryInfo(entity_1, 21), nullptr);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
