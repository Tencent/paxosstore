#include "utils/hash.h"

#include "gtest/gtest.h"

TEST(HashTest, Basic) {
  ASSERT_TRUE(certain::Hash("hello") != certain::Hash("123456789"));
  ASSERT_TRUE(certain::Hash("hello") == certain::Hash("hello"));

  std::unordered_map<uint32_t, bool> hash_map;
  for (uint64_t i = 0; i < 100000; ++i) {
    hash_map[certain::Hash(i)] = true;
  }
  ASSERT_EQ(hash_map.size(), 100000);
}

TEST(HashTest, Performance_1000k_times) {
  for (uint64_t i = 0; i < 1000000; ++i) {
    certain::Hash(i);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
