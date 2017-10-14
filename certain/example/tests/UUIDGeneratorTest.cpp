#include "example/UUIDGenerator.h"

#include <string>
#include <sys/poll.h>
#include <gtest/gtest.h>

TEST(clsUUIDGeneratorTest, UUIDTest)
{
    // When it is initialized, GetUUID will update counter.
    clsUUIDGenerator::GetInstance()->Init();
    uint64_t iUUID1 = clsUUIDGenerator::GetInstance()->GetUUID();
    uint64_t iUUID2 = clsUUIDGenerator::GetInstance()->GetUUID();
    EXPECT_NE(iUUID1, iUUID2);

    // When it is initialized, the first value of counter will be changed.
    clsUUIDGenerator::GetInstance()->Init();
    uint64_t iUUID3 = clsUUIDGenerator::GetInstance()->GetUUID();
    EXPECT_NE(iUUID1, iUUID3);
    EXPECT_NE(iUUID2, iUUID3);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
