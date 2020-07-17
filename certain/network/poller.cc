#include "poller.h"

namespace certain {

Poller::Poller(int event_size) {
  event_size_ = event_size;
  epoll_events_ = (epoll_event*)calloc(event_size, sizeof(epoll_event));
  assert(epoll_events_ != NULL);

  epoll_fd_ = epoll_create(1024);
  if (epoll_fd_ == -1) {
    CERTAIN_LOG_FATAL("epoll_create failed epoll_fd %d errno [%s]", epoll_fd_,
                      strerror(errno));
    assert(false);
  }
}

Poller::~Poller() {
  free(epoll_events_);

  if (epoll_fd_ >= 0) {
    close(epoll_fd_);
  }
}

int Poller::Add(FdObj* fd_obj) {
  int fd = fd_obj->fd();

  epoll_event ev = {0};
  ev.events = fd_obj->events();
  ev.data.ptr = static_cast<void*>(fd_obj);

  int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("epoll_ctl failed fd %d errno [%s]", fd, strerror(errno));
    return kNetWorkError;
  }

  return 0;
}

int Poller::Remove(FdObj* fd_obj) {
  int fd = fd_obj->fd();

  int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, NULL);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("epoll_ctl failed fd %d errno [%s]", fd, strerror(errno));
    return kNetWorkError;
  }

  return 0;
}

int Poller::RemoveAndCloseFd(FdObj* fd_obj) {
  int ret;
  int fd = fd_obj->fd();

  ret = Remove(fd_obj);
  if (ret != 0) {
    CERTAIN_LOG_ERROR("Remove ret %d", ret);
    return kNetWorkError;
  }

  ret = close(fd);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("close failed fd %d errno [%s]", fd, strerror(errno));
    return kNetWorkError;
  }

  return 0;
}

int Poller::Modify(FdObj* fd_obj) {
  int fd = fd_obj->fd();

  epoll_event ev = {0};
  ev.events = fd_obj->events();
  ev.data.ptr = static_cast<void*>(fd_obj);

  int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
  if (ret == -1) {
    CERTAIN_LOG_FATAL("epoll_ctl failed fd %d errno [%s]", fd, strerror(errno));
    return kNetWorkError;
  }

  return 0;
}

void Poller::RunOnce(int msec) {
  int num, ret;

  while (1) {
    num = epoll_wait(epoll_fd_, epoll_events_, event_size_, msec);
    if (num == -1) {
      if (errno == EINTR) {
        continue;
      }

      CERTAIN_LOG_FATAL("epoll_wait failed fd %d errno [%s]", epoll_fd_,
                        strerror(errno));
      assert(false);
    }
    break;
  }

  // With  edge-triggered  epoll, all the events(i.e. readable and writable)
  // can be shown upon receipt of one event(i.e. readable).

  for (int i = 0; i < num; ++i) {
    int events = epoll_events_[i].events;
    FdObj* fd_obj = static_cast<FdObj*>(epoll_events_[i].data.ptr);
    HandlerBase* handler = fd_obj->handler();
    assert(handler != NULL);

    if ((events & EPOLLIN) || (events & EPOLLERR) || (events & EPOLLHUP)) {
      fd_obj->set_readable(true);
      ret = handler->HandleRead(fd_obj);
      if (ret != 0) {
        continue;
      }
    }

    if (events & EPOLLOUT) {
      fd_obj->set_writable(true);
      ret = handler->HandleWrite(fd_obj);
      if (ret != 0) {
        continue;
      }
    }
  }
}

}  // namespace certain
