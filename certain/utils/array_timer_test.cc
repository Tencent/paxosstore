#include "array_timer.h"

#include "gtest/gtest.h"

struct Foo {
  int bar;

  certain::ArrayTimer<Foo>::EltEntry timer_entry;

  Foo(int _bar) { bar = _bar; }
};

TEST(ArrayTimerTest, Basic) {
  Foo foo1(1);
  Foo foo2(2);
  Foo foo3(3);

  {
    certain::ArrayTimer<Foo> timer(0);

    ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
    timer.Add(&foo1, 0);
    timer.Add(&foo2, 0);
    timer.Add(&foo3, 0);
    ASSERT_EQ(timer.Size(), 3);

    ASSERT_EQ(timer.TakeTimerElt()->bar, 1);
    ASSERT_EQ(timer.TakeTimerElt()->bar, 2);
    ASSERT_EQ(timer.TakeTimerElt()->bar, 3);
    ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
    ASSERT_EQ(timer.Size(), 0);
  }

  {
    certain::ArrayTimer<Foo> timer(1);

    ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
    timer.Add(&foo1, 1);
    timer.Add(&foo2, 0);
    timer.Add(&foo3, 0);
    ASSERT_EQ(timer.Size(), 3);

    ASSERT_EQ(timer.TakeTimerElt()->bar, 2);
    ASSERT_EQ(timer.TakeTimerElt()->bar, 3);
    ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
    usleep(1000);
    ASSERT_EQ(timer.TakeTimerElt()->bar, 1);
    usleep(1000);
    ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
    ASSERT_EQ(timer.Size(), 0);
  }
}

TEST(ArrayTimerTest, LongTime) {
  Foo foo1(1);
  Foo foo2(2);
  Foo foo3(3);
  Foo foo4(4);

  certain::ArrayTimer<Foo> timer(1000);

  ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
  timer.Add(&foo1, 1000);
  timer.Add(&foo2, 0);
  timer.Add(&foo3, 0);
  timer.Add(&foo4, 999);

  ASSERT_EQ(timer.TakeTimerElt()->bar, 2);
  ASSERT_EQ(timer.TakeTimerElt()->bar, 3);
  ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
  usleep(1000 * 999);
  ASSERT_EQ(timer.TakeTimerElt()->bar, 4);
  ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
  ASSERT_EQ(timer.Size(), 1);
  timer.Add(&foo2, 1);
  timer.Add(&foo3, 3);
  usleep(1000);
  ASSERT_EQ(timer.TakeTimerElt()->bar, 1);
  ASSERT_EQ(timer.TakeTimerElt()->bar, 2);
  ASSERT_TRUE(timer.TakeTimerElt() == nullptr);
  usleep(2000);
  ASSERT_EQ(timer.TakeTimerElt()->bar, 3);
  ASSERT_EQ(timer.Size(), 0);
}

TEST(ArrayTimerTest, Performance_1000k_Add) {
  uint32_t max_timeout_msec = 60000;
  certain::ArrayTimer<Foo> timer(max_timeout_msec);
  for (int i = 0; i < 1000000; ++i) {
    Foo* foo = new Foo(0);
    timer.Add(foo, i % max_timeout_msec);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
