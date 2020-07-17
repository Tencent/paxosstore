#include "utils/light_list.h"

#include "gtest/gtest.h"

struct Foo {
  int bar;

  LIGHTLIST_ENTRY(Foo) test_list_entry;

  Foo() = delete;

  Foo(int _bar) {
    bar = _bar;
    LIGHTLIST_ENTRY_INIT(this, test_list_entry);
  }
};

TEST(LightListTest, Basic) {
  LIGHTLIST(Foo) test_list;
  LIGHTLIST_INIT(&test_list);

  Foo e1(1);
  Foo e2(2);
  Foo e3(3);

  // Insert e1 and than remove it.
  ASSERT_FALSE(ENTRY_IN_LIGHTLIST(&e1, test_list_entry));
  LIGHTLIST_INSERT_HEAD(&test_list, &e1, test_list_entry);
  ASSERT_TRUE(ENTRY_IN_LIGHTLIST(&e1, test_list_entry));
  LIGHTLIST_REMOVE(&test_list, &e1, test_list_entry);
  ASSERT_FALSE(ENTRY_IN_LIGHTLIST(&e1, test_list_entry));

  LIGHTLIST_INSERT_HEAD(&test_list, &e1, test_list_entry);
  LIGHTLIST_INSERT_HEAD(&test_list, &e2, test_list_entry);
  LIGHTLIST_INSERT_HEAD(&test_list, &e3, test_list_entry);

  ASSERT_FALSE(LIGHTLIST_EMPTY(&test_list));

  // Check e3.
  ASSERT_EQ(LIGHTLIST_FIRST(&test_list)->bar, 3);
  ASSERT_TRUE(ENTRY_IN_LIGHTLIST(&e3, test_list_entry));
  LIGHTLIST_REMOVE(&test_list, &e3, test_list_entry);
  ASSERT_FALSE(ENTRY_IN_LIGHTLIST(&e3, test_list_entry));

  // Check e2.
  ASSERT_EQ(LIGHTLIST_FIRST(&test_list)->bar, 2);
  ASSERT_TRUE(ENTRY_IN_LIGHTLIST(&e2, test_list_entry));
  LIGHTLIST_REMOVE(&test_list, &e2, test_list_entry);
  ASSERT_FALSE(ENTRY_IN_LIGHTLIST(&e2, test_list_entry));

  // Check e1.
  ASSERT_EQ(LIGHTLIST_FIRST(&test_list)->bar, 1);
  ASSERT_TRUE(ENTRY_IN_LIGHTLIST(&e1, test_list_entry));
  LIGHTLIST_REMOVE(&test_list, &e1, test_list_entry);
  ASSERT_FALSE(ENTRY_IN_LIGHTLIST(&e1, test_list_entry));

  ASSERT_TRUE(LIGHTLIST_EMPTY(&test_list));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
