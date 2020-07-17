#pragma once
#include "utils/macro_helper.h"

namespace certain {

template <typename T>
class Singleton {
 public:
  Singleton() {}

  static T* GetInstance() {
    static T instance;
    return &instance;
  }

 private:
  CERTAIN_NO_COPYING_ALLOWED(Singleton);
};

}  // namespace certain
