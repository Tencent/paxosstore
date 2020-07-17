#include "utils/lru_table.h"

#include "gtest/gtest.h"

TEST(LruTable, Basic) {
  certain::LruTable<int, std::string> lt;
  ASSERT_EQ(lt.Size(), 0);

  lt.Add(1, "11");
  lt.Add(2, "22");
  lt.Add(3, "33");
  lt.Add(4, "44");
  ASSERT_EQ(lt.Size(), 4);

  int key;
  std::string value;
  ASSERT_TRUE(lt.PeekOldest(key, value));
  ASSERT_EQ(key, 1);
  ASSERT_STREQ(value.c_str(), "11");

  lt.Refresh(3, false);
  ASSERT_TRUE(lt.PeekOldest(key, value));
  ASSERT_EQ(key, 3);
  ASSERT_STREQ(value.c_str(), "33");
  ASSERT_EQ(lt.Size(), 4);

  ASSERT_TRUE(lt.RemoveOldest());
  ASSERT_EQ(lt.Size(), 3);
  ASSERT_TRUE(lt.PeekOldest(key, value));
  ASSERT_EQ(key, 1);
  ASSERT_STREQ(value.c_str(), "11");

  ASSERT_TRUE(lt.Remove(2));
  lt.Refresh(4, false);

  ASSERT_TRUE(lt.PeekOldest(key, value));
  ASSERT_EQ(key, 4);
  ASSERT_STREQ(value.c_str(), "44");
  ASSERT_TRUE(lt.RemoveOldest());

  ASSERT_TRUE(lt.PeekOldest(key, value));
  ASSERT_EQ(key, 1);
  ASSERT_STREQ(value.c_str(), "11");
  ASSERT_TRUE(lt.RemoveOldest());
  ASSERT_EQ(lt.Size(), 0);
}

TEST(LruTable, Performance_1000k_Add_RemoveOldest) {
  int size = 1000000;
  certain::LruTable<uint64_t, uint64_t> lt(size);
  for (int i = 0; i < size; ++i) {
    lt.Add(i, i);
  }
  ASSERT_EQ(lt.Size(), size);
  for (int i = 0; i < size; ++i) {
    lt.RemoveOldest();
  }
  ASSERT_EQ(lt.Size(), 0);
}

TEST(LruTable, Performance_1000k_Auto_Eliminate) {
  int size = 1000000;
  certain::LruTable<uint64_t, uint64_t> lt(size, true);
  for (int i = 0; i < size * 2; ++i) {
    lt.Add(i, i);
  }
  ASSERT_EQ(lt.Size(), size);
  for (int i = 0; i < size; ++i) {
    lt.RemoveOldest();
  }
  ASSERT_EQ(lt.Size(), 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
