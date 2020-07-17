#include "src/entity_info_mng.h"

#include "gtest/gtest.h"

TEST(EntityInfoMng, Normal) {
  certain::Options options;
  certain::EntityInfoMng mng(&options);

  uint32_t acceptor_num_1 = 3;
  uint32_t acceptor_num_2 = 5;
  uint32_t local_acceptor_id_1 = 2;
  uint32_t local_acceptor_id_2 = 4;

  ASSERT_EQ(mng.FindEntityInfo(1), nullptr);
  ASSERT_EQ(mng.FindEntityInfo(2), nullptr);
  ASSERT_NE(mng.CreateEntityInfo(2, acceptor_num_2, local_acceptor_id_2),
            nullptr);
  ASSERT_EQ(mng.FindEntityInfo(1), nullptr);
  ASSERT_NE(mng.FindEntityInfo(2), nullptr);
  ASSERT_NE(mng.CreateEntityInfo(1, acceptor_num_1, local_acceptor_id_1),
            nullptr);
  certain::EntityInfo* entity_info = nullptr;

  entity_info = mng.FindEntityInfo(1);
  ASSERT_EQ(entity_info->acceptor_num, acceptor_num_1);
  ASSERT_EQ(entity_info->local_acceptor_id, local_acceptor_id_1);
  entity_info = mng.FindEntityInfo(2);
  ASSERT_EQ(entity_info->acceptor_num, acceptor_num_2);
  ASSERT_EQ(entity_info->local_acceptor_id, local_acceptor_id_2);

  mng.DestroyEntityInfo(entity_info);
  ASSERT_EQ(mng.FindEntityInfo(2), nullptr);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
