#include "network/write_item_list.h"

#include "gtest/gtest.h"

class WriteItemListTest : public testing::Test {
 public:
  virtual void SetUp() override {
    write_item_list_.AddWriteItem(std::make_unique<char[]>(1), 1);
    write_item_list_.AddWriteItem(std::make_unique<char[]>(10), 10);
    write_item_list_.AddWriteItem(std::make_unique<char[]>(100), 100);
    write_item_list_.AddWriteItem(std::make_unique<char[]>(1000), 1000);
    write_item_list_.AddWriteItem(std::make_unique<char[]>(10000), 10000);
  }

  virtual void TearDown() override { write_item_list_.FreeAllWriteItems(); }

 protected:
  certain::WriteItemList write_item_list_;
};

TEST_F(WriteItemListTest, Basic) {
  ASSERT_FALSE(write_item_list_.empty());
  ASSERT_EQ(write_item_list_.CleanWrittenItems(), 0);
  ASSERT_EQ(write_item_list_.FreeAllWriteItems(), 11111);
  ASSERT_TRUE(write_item_list_.empty());
}

TEST_F(WriteItemListTest, GetFirstNIterms) {
  struct iovec io_vec[3];
  ASSERT_EQ(write_item_list_.GetFirstNIterms(io_vec, 3), 3);
  ASSERT_EQ(write_item_list_.CleanWrittenItems(), 0);

  ASSERT_EQ(write_item_list_.GetFirstNIterms(io_vec, 3), 3);
  io_vec[0].iov_base = (char*)io_vec[0].iov_base + 1;
  io_vec[0].iov_len = 0;
  io_vec[1].iov_base = (char*)io_vec[1].iov_base + 10;
  io_vec[1].iov_len = 0;
  io_vec[2].iov_base = (char*)io_vec[2].iov_base + 100;
  io_vec[2].iov_len = 0;
  ASSERT_EQ(write_item_list_.CleanWrittenItems(), 111);

  ASSERT_EQ(write_item_list_.GetFirstNIterms(io_vec, 3), 2);
  io_vec[0].iov_base = (char*)io_vec[0].iov_base + 1000;
  io_vec[0].iov_len = 0;
  io_vec[1].iov_base = (char*)io_vec[1].iov_base + 10000;
  io_vec[1].iov_len = 0;
  ASSERT_EQ(write_item_list_.CleanWrittenItems(), 11000);
  ASSERT_EQ(write_item_list_.FreeAllWriteItems(), 0);
}

TEST_F(WriteItemListTest, CleanHalfItem) {
  struct iovec io_vec[5];
  ASSERT_EQ(write_item_list_.GetFirstNIterms(io_vec, 3), 3);
  io_vec[0].iov_base = (char*)io_vec[0].iov_base + 1;
  io_vec[0].iov_len = 0;
  io_vec[1].iov_base = (char*)io_vec[1].iov_base + 8;
  io_vec[1].iov_len = 2;
  io_vec[2].iov_base = (char*)io_vec[2].iov_base + 90;
  io_vec[2].iov_len = 10;
  ASSERT_EQ(write_item_list_.CleanWrittenItems(), 1);
  ASSERT_EQ(write_item_list_.GetFirstNIterms(io_vec, 5), 4);
  io_vec[0].iov_base = (char*)io_vec[0].iov_base + 2;
  io_vec[0].iov_len = 0;
  ASSERT_EQ(write_item_list_.CleanWrittenItems(), 10);
  ASSERT_EQ(write_item_list_.FreeAllWriteItems(), 11100);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
