#include "utils/FixSizePool.h"
#include "gtest/gtest.h"

using namespace Certain;

TEST(FixSizePoolTest, Basic)
{
    clsFixSizePool o(2, 100);

    char *ptr0 = NULL;
    char *ptr1 = NULL;
    char *ptr2 = NULL;

    ptr0 = o.Alloc(100);
    EXPECT_TRUE(ptr0 != NULL);

    ptr1 = o.Alloc(100);
    EXPECT_TRUE(ptr1 != ptr0);

    o.Free(ptr0);
    ptr2 = o.Alloc(100);
    EXPECT_TRUE(ptr2 == ptr0);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
