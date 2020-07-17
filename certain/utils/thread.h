#pragma once

#include "utils/header.h"
#include "utils/log.h"
#include "utils/macro_helper.h"

namespace certain {

class ThreadBase {
 private:
  std::unique_ptr<std::thread> thread_;
  std::string name_;

  std::atomic<bool> exit_flag_;
  std::atomic<bool> stopped_;

  CERTAIN_NO_COPYING_ALLOWED(ThreadBase);

 public:
  ThreadBase();
  ThreadBase(std::string name);
  virtual ~ThreadBase();

  // set_exit_flag Called by the thread called Start() only.
  CERTAIN_GET_SET(std::string, name);

  CERTAIN_GET_SET_ATOMIC(bool, exit_flag);

  CERTAIN_GET_SET_ATOMIC(bool, stopped);

  static void* ThreadEntry(void* arg);

  // Call Start() to call Run() in the thread.
  void Start();

  virtual bool Started() const { return thread_ != nullptr; }
  virtual bool Stopped() const { return stopped_.load(); }

  // Maybe implemented to wait until all threads created by this thread. The
  // default implementation is to simply join the thread.
  virtual void WaitExit();

  virtual void Run() = 0;

  static void SetThreadName(const char* fmt, ...);
  static int GetProcessorNum();
  static void SetAffinity(std::vector<uint32_t> cores);
};

class Mutex {
 public:
  Mutex() { mutex_ = PTHREAD_MUTEX_INITIALIZER; }
  ~Mutex() {}

  void Lock() {
    int ret = pthread_mutex_lock(&mutex_);
    assert(ret == 0);
  }

  void Unlock() {
    int ret = pthread_mutex_unlock(&mutex_);
    assert(ret == 0);
  }

 private:
  pthread_mutex_t mutex_;
  CERTAIN_NO_COPYING_ALLOWED(Mutex);
};

class ThreadLock {
 public:
  explicit ThreadLock(Mutex* mutex) : mutex_(mutex) { mutex_->Lock(); }
  ~ThreadLock() { mutex_->Unlock(); }

 private:
  Mutex* mutex_;
  CERTAIN_NO_COPYING_ALLOWED(ThreadLock);
};

class ReadWriteLock {
 public:
  ReadWriteLock() { rwlock_ = PTHREAD_RWLOCK_INITIALIZER; }
  ~ReadWriteLock() {}

  void ReadLock() {
    int ret = pthread_rwlock_rdlock(&rwlock_);
    assert(ret == 0);
  }

  void WriteLock() {
    int ret = pthread_rwlock_wrlock(&rwlock_);
    assert(ret == 0);
  }

  void Unlock() {
    int ret = pthread_rwlock_unlock(&rwlock_);
    assert(ret == 0);
  }

 private:
  pthread_rwlock_t rwlock_;
  CERTAIN_NO_COPYING_ALLOWED(ReadWriteLock);
};

class ThreadReadLock {
 public:
  explicit ThreadReadLock(ReadWriteLock* rwlock) : rwlock_(rwlock) {
    rwlock_->ReadLock();
  }
  ~ThreadReadLock() { rwlock_->Unlock(); }

 private:
  ReadWriteLock* rwlock_;
  CERTAIN_NO_COPYING_ALLOWED(ThreadReadLock);
};

class ThreadWriteLock {
 public:
  explicit ThreadWriteLock(ReadWriteLock* rwlock) : rwlock_(rwlock) {
    rwlock_->WriteLock();
  }
  ~ThreadWriteLock() { rwlock_->Unlock(); }

 private:
  ReadWriteLock* rwlock_;
  CERTAIN_NO_COPYING_ALLOWED(ThreadWriteLock);
};

}  // namespace certain
