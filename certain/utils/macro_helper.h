#pragma once

// There is always a fatal log before panic for detail.
#define CERTAIN_PANIC() \
  {                     \
    sleep(2);           \
    assert(false);      \
  }

#define CERTAIN_UNREACHABLE() \
  { assert(false); }

#define CERTAIN_NO_COPYING_ALLOWED(cls) \
  cls(const cls&) = delete;             \
  cls& operator=(const cls&) = delete;

#define CERTAIN_GET_SET(type, name)            \
  const type& name() const { return name##_; } \
  void set_##name(const type& name) { name##_ = name; }

#define CERTAIN_GET_SET_ATOMIC(type, name)     \
  type name() const { return name##_.load(); } \
  void set_##name(const type& name) { name##_.store(name); }

#define CERTAIN_GET_SET_PTR(type, name) \
  type name() const { return name##_; } \
  void set_##name(type name) { name##_ = name; }
