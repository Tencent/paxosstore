#include "utils/mem_pool.h"

#include "gtest/gtest.h"

TEST(MemPoolTest, Basic) {
  certain::MemPool pool(3, 5);
  std::cout << pool.ToString() << std::endl;
  ASSERT_TRUE(pool.Alloc(0) == NULL);

  char* p1 = pool.Alloc(3);
  char* p2 = pool.Alloc(4);
  char* p3 = pool.Alloc(5);
  char* p4 = pool.Alloc(3);
  char* p5 = pool.Alloc(6);
  ASSERT_EQ(pool.pool_alloc_cnt(), 3);
  ASSERT_EQ(pool.os_alloc_cnt(), 2);

  pool.Free(p1);
  pool.Free(p2);
  pool.Free(p4);

  ASSERT_EQ(pool.pool_alloc_cnt(), 1);
  ASSERT_EQ(pool.os_alloc_cnt(), 1);

  pool.Free(p3);
  pool.Free(p5);

  pool.Alloc(3);
  pool.Alloc(4);
  pool.Alloc(5);
  pool.Alloc(3);
  pool.Alloc(6);
  ASSERT_EQ(pool.pool_alloc_cnt(), 3);
  ASSERT_EQ(pool.os_alloc_cnt(), 2);
}

TEST(MemPoolTest, Performance_10000k_Alloc_Free_1KB) {
  // need ~1GB memory
  int size = 1000000;
  certain::MemPool pool(size, 1000);
  std::vector<char*> vc;
  for (int i = 0; i < size; ++i) {
    vc.push_back(pool.Alloc(1000));
  }
  for (int i = 0; i < size; ++i) {
    pool.Free(vc[i]);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
