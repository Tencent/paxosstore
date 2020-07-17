#pragma once

#include "utils/light_list.h"
#include "utils/time.h"

namespace certain {

template <typename T>
class ArrayTimer {
 public:
  struct EltEntry {
    uint32_t index;
    LIGHTLIST_ENTRY(T) timer_list_entry;

    void Init() {
      index = 0;  // For Remove method.
      LIGHTLIST_ENTRY_INIT(this, timer_list_entry);
    }
    EltEntry() { Init(); }
  };

 private:
  const uint32_t kReadyListIndex = uint32_t(-1);

  uint32_t timer_cnt_;
  uint32_t ready_cnt_;

  uint32_t max_timeout_msec_;
  uint32_t curr_index_;
  uint64_t curr_time_msec_;

  typedef LIGHTLIST(T) TimerList;
  TimerList* timer_list_;
  TimerList* ready_list_;

  // Return how many timer moved.
  uint32_t MoveTimerToReadyListByIndex(uint32_t index) {
    assert(index <= max_timeout_msec_);
    TimerList* curr_list = &timer_list_[index];

    uint32_t moved_cnt = 0;
    while (!LIGHTLIST_EMPTY(curr_list)) {
      T* last = LIGHTLIST_LAST(curr_list);

      LIGHTLIST_REMOVE(curr_list, last, timer_entry.timer_list_entry);
      assert(last->timer_entry.index == index);

      last->timer_entry.index = kReadyListIndex;
      LIGHTLIST_INSERT_HEAD(ready_list_, last, timer_entry.timer_list_entry);
      moved_cnt++;
    }

    return moved_cnt;
  }

  // Move add all timers before curr_time_msec to ready_list_, then update
  // curr_time_msec_, curr_index_, timer_cnt_ and ready_cnt_.
  void MoveTimerToReadyList(uint64_t curr_time_msec) {
    // Fix curr_time_msec if it's not monotonnous.
    if (curr_time_msec < curr_time_msec_) {
      curr_time_msec = curr_time_msec_;
    }

    uint64_t range = curr_time_msec - curr_time_msec_;
    if (range > max_timeout_msec_) {
      range = max_timeout_msec_;
    }

    uint32_t index = curr_index_;
    uint32_t moved_cnt = 0;
    for (uint32_t i = 0; i <= range; ++i) {
      index = i + curr_index_;

      // timer_list_ array is cyclic reused.
      if (index >= max_timeout_msec_ + 1) {
        index -= max_timeout_msec_ + 1;
      }

      moved_cnt += MoveTimerToReadyListByIndex(index);
    }

    ready_cnt_ += moved_cnt;
    timer_cnt_ -= moved_cnt;
    curr_time_msec_ = curr_time_msec;
    curr_index_ = index;
  }

 public:
  // Support timer alarms in [0, max_timeout_msec].
  ArrayTimer(uint32_t max_timeout_msec) {
    timer_cnt_ = 0;
    ready_cnt_ = 0;
    max_timeout_msec_ = max_timeout_msec;

    timer_list_ = (TimerList*)calloc(max_timeout_msec_ + 1, sizeof(TimerList));
    assert(timer_list_ != NULL);

    for (uint32_t i = 0; i <= max_timeout_msec_; ++i) {
      LIGHTLIST_INIT(&timer_list_[i]);
    }

    ready_list_ = (TimerList*)calloc(1, sizeof(TimerList));
    LIGHTLIST_INIT(ready_list_);

    curr_index_ = 0;
    curr_time_msec_ = GetTimeByMsec();
  }

  ~ArrayTimer() {
    free(ready_list_);
    free(timer_list_);
  }

  uint32_t Size() { return timer_cnt_ + ready_cnt_; }

  bool Add(T* elt, uint32_t timeout_msec) {
    assert(timeout_msec <= max_timeout_msec_);

    if (Exist(elt)) {
      return false;
    }

    uint64_t msec = GetTimeByMsec();
    MoveTimerToReadyList(msec);

    uint32_t index = curr_index_ + timeout_msec;
    if (index >= max_timeout_msec_ + 1) {
      index -= (max_timeout_msec_ + 1);
    }

    elt->timer_entry.index = index;
    LIGHTLIST_INSERT_HEAD(&timer_list_[index], elt,
                          timer_entry.timer_list_entry);
    timer_cnt_++;

    return true;
  }

  bool Remove(T* elt) {
    if (!Exist(elt)) {
      assert(elt->timer_entry.index == 0);
      return false;
    }

    TimerList* timer_list = ready_list_;
    if (elt->timer_entry.index != kReadyListIndex) {
      timer_list = &timer_list_[elt->timer_entry.index];
      timer_cnt_--;
    } else {
      ready_cnt_--;
    }

    LIGHTLIST_REMOVE(timer_list, elt, timer_entry.timer_list_entry);
    elt->timer_entry.Init();

    return true;
  }

  T* TakeTimerElt() {
    if (ready_cnt_ == 0) {
      MoveTimerToReadyList(GetTimeByMsec());
      if (ready_cnt_ == 0) {
        return NULL;
      }
    }

    T* elt = LIGHTLIST_LAST(ready_list_);
    bool bRemoved = Remove(elt);
    assert(bRemoved);

    return elt;
  }

  bool Exist(T* elt) {
    if (ENTRY_IN_LIGHTLIST(elt, timer_entry.timer_list_entry)) {
      return true;
    } else {
      assert(elt->timer_entry.index == 0);
      return false;
    }
  }
};

}  // namespace certain
