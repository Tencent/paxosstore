#include "utils/thread.h"

#include "utils/memory.h"

namespace certain {

ThreadBase::ThreadBase() {
  exit_flag_.store(false);
  stopped_.store(false);
}

ThreadBase::ThreadBase(std::string name) {
  exit_flag_.store(false);
  stopped_.store(false);
  name_ = name;
}

ThreadBase::~ThreadBase() {}

void* ThreadBase::ThreadEntry(void* arg) {
  ThreadBase* worker = static_cast<ThreadBase*>(arg);
  if (!worker->name_.empty()) {
    SetThreadName(worker->name_.c_str());
  }

  CERTAIN_LOG_ZERO("%s worker start running.", worker->name_.c_str());
  worker->Run();
  worker->set_stopped(true);
  CERTAIN_LOG_ZERO("%s worker exit.", worker->name_.c_str());

  return NULL;
}

void ThreadBase::Start() {
  thread_ = std::make_unique<std::thread>(ThreadEntry, this);
}

// Maybe implemented to wait until all threads created by this thread. The
// default implementation is to simply join the thread.
void ThreadBase::WaitExit() {
  if (thread_ == NULL) {
    return;
  }
  thread_->join();
  stopped_ = true;
}

void ThreadBase::SetThreadName(const char* fmt, ...) {
  char name[16] = {0};

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(name, sizeof(name), fmt, ap);
  va_end(ap);

  int ret = prctl(PR_SET_NAME, name);
  assert(ret == 0);
}

int ThreadBase::GetProcessorNum() { return get_nprocs(); }

void ThreadBase::SetAffinity(std::vector<uint32_t> cores) {
  cpu_set_t mask;
  CPU_ZERO(&mask);

  for (auto i : cores) {
    CPU_SET(i, &mask);
  }

  pthread_t t = pthread_self();
  int ret = pthread_setaffinity_np(t, sizeof(mask), &mask);
  assert(ret == 0);
}

}  //  namespace certain
