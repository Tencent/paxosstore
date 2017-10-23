#include "LeasePolicy.h"
#include <gtest/gtest.h>

using namespace Certain;

TEST(LeasePolicyTest, NormalTest)
{
    {
        clsLeasePolicy oPolicy(1, 0);
        EXPECT_EQ(oPolicy.GetLeaseTimeoutMS(), 0);

        oPolicy.OnRecvMsgSuccessfully();
        EXPECT_EQ(oPolicy.GetLeaseTimeoutMS(), 0);
    }

    {
        clsLeasePolicy oPolicy(1, 10);
        EXPECT_EQ(oPolicy.GetLeaseTimeoutMS(), 0);
        usleep(10 * 1000);
        EXPECT_EQ(oPolicy.GetLeaseTimeoutMS(), 0);

        oPolicy.OnRecvMsgSuccessfully();
        EXPECT_EQ(oPolicy.GetLeaseTimeoutMS(), 10);
        usleep(10 * 1000);
        EXPECT_EQ(oPolicy.GetLeaseTimeoutMS(), 0);
    }
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
