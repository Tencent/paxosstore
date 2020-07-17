#include "utils/lock_free_queue.h"

#include "gtest/gtest.h"

TEST(LockFreeQueueTest, Basic) {
  certain::LockFreeQueue<uint64_t> q(1);

  ASSERT_EQ(q.Size(), 0);
  ASSERT_FALSE(q.Full());

  auto item = std::make_unique<uint64_t>(1);
  ASSERT_EQ(q.PushByMultiThread(&item), 0);
  item = std::make_unique<uint64_t>(2);
  ASSERT_EQ(q.PushByMultiThread(&item), certain::kUtilsQueueFull);
  ASSERT_EQ(*item, 2);

  ASSERT_EQ(q.Size(), 1);
  ASSERT_TRUE(q.Full());

  ASSERT_EQ(q.PopByOneThread(&item), 0);
  ASSERT_EQ(q.PopByOneThread(&item), certain::kUtilsQueueEmpty);
  ASSERT_EQ(*item, 1);

  ASSERT_EQ(q.Size(), 0);
  ASSERT_FALSE(q.Full());
}

TEST(LockFreeQueueTest, Large) {
  uint64_t size = 100000;
  certain::LockFreeQueue<uint64_t> q(size);
  for (uint64_t i = 1; i <= size; ++i) {
    auto item = std::make_unique<uint64_t>(i);
    ASSERT_EQ(q.PushByMultiThread(&item), 0);
  }

  ASSERT_EQ(q.Size(), size);
  auto item = std::make_unique<uint64_t>(0);
  ASSERT_EQ(q.PushByMultiThread(&item), certain::kUtilsQueueFull);
  ASSERT_TRUE(q.Full());

  for (uint64_t i = 1; i <= size; ++i) {
    std::unique_ptr<uint64_t> item;
    ASSERT_EQ(q.PopByOneThread(&item), 0);
    ASSERT_EQ(*item, i);
  }

  ASSERT_EQ(q.Size(), 0);
}

TEST(LockFreeQueueTest, MultiThread2) {
  uint64_t size = 2 * 1000000;
  certain::LockFreeQueue<uint64_t> q(size);

  std::thread push_worker[2];
  for (int i = 0; i < 2; ++i) {
    push_worker[i] = std::thread([&] {
      uint64_t conflict_count = 0;
      for (uint64_t i = 1; i <= 1000000; ++i) {
        auto item = std::make_unique<uint64_t>(i);
        while (q.PushByMultiThread(&item) != 0) {
          conflict_count++;
          continue;
        }
      }
      printf("conflict_count %lu\n", conflict_count);
    });
  }

  for (uint64_t i = 0; i < size; ++i) {
    std::unique_ptr<uint64_t> item;
    while (q.PopByOneThread(&item) != 0) {
      continue;
    }
  }

  ASSERT_EQ(q.Size(), 0);

  for (int i = 0; i < 2; ++i) {
    push_worker[i].join();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
