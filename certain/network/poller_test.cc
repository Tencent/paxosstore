#include "network/poller.h"

#include "gtest/gtest.h"

class FooHandler : public certain::HandlerBase {
 public:
  FooHandler() {
    read_count_ = 0;
    written_count_ = 0;
  }

  virtual ~FooHandler() {}

  virtual int HandleRead(certain::FdObj* fd_obj) override {
    assert(fd_obj->readable());
    char buf[100] = {0};
    read(fd_obj->fd(), buf, 100);
    printf("HandleRead fd %d buf %s\n", fd_obj->fd(), buf);
    assert(std::string(buf) == "hello" || std::string(buf) == "hellohello");
    read_count_++;
    return 0;
  }

  virtual int HandleWrite(certain::FdObj* fd_obj) override {
    assert(fd_obj->writable());
    char buf[100] = "hello";
    write(fd_obj->fd(), buf, strlen(buf));
    printf("HandleWrite fd %d buf %s\n", fd_obj->fd(), buf);
    assert(std::string(buf) == "hello" || std::string(buf) == "hellohello");
    written_count_++;
    return 0;
  }

  CERTAIN_GET_SET(int, read_count);
  CERTAIN_GET_SET(int, written_count);

 private:
  int read_count_;
  int written_count_;
};

TEST(PollerTest, Basic) {
  certain::Poller epoll_io;

  certain::InetAddr addr1("127.0.0.1", 13768);

  certain::TcpSocket tcp1;
  ASSERT_EQ(tcp1.InitSocket(), 0);
  ASSERT_EQ(tcp1.Bind(addr1), 0);
  ASSERT_EQ(tcp1.Listen(), 0);
  std::cout << tcp1.ToString() << std::endl;

  ASSERT_TRUE(tcp1.listened());

  certain::TcpSocket tcp2;
  certain::InetAddr addr2("127.0.0.1", 13769);
  ASSERT_EQ(tcp2.InitSocket(), 0);
  ASSERT_EQ(tcp2.Bind(addr2), 0);
  ASSERT_EQ(tcp2.Connect(addr1), certain::kNetWorkInProgress);
  std::cout << tcp2.ToString() << std::endl;

  certain::InetAddr addr;
  int fd = tcp1.Accept(addr);
  ASSERT_GT(fd, 0);
  ASSERT_TRUE(addr == addr2);

  certain::TcpSocket* tcp3 = new certain::TcpSocket(fd, addr1, addr2);
  ASSERT_EQ(tcp3->InitSocket(), 0);
  std::cout << tcp3->ToString() << std::endl;

  FooHandler handler2;
  FooHandler handler3;

  auto fd_obj1 = new certain::FdObj(tcp2.fd(), &handler2);
  auto fd_obj2 = new certain::FdObj(tcp3->fd(), &handler3);
  ASSERT_EQ(epoll_io.Add(fd_obj1), 0);
  ASSERT_EQ(epoll_io.Add(fd_obj2), 0);
  ASSERT_EQ(epoll_io.Modify(fd_obj2), 0);

  epoll_io.RunOnce(1);
  epoll_io.RunOnce(1);
  epoll_io.RunOnce(1);
  epoll_io.RunOnce(1);

  ASSERT_EQ(handler3.written_count(), 4);
  ASSERT_EQ(handler2.written_count(), 4);

  ASSERT_EQ(epoll_io.RemoveAndCloseFd(fd_obj1), 0);

  epoll_io.RunOnce(1);
  ASSERT_EQ(handler2.written_count(), 4);
  ASSERT_EQ(handler3.written_count(), 5);

  ASSERT_EQ(epoll_io.Remove(fd_obj1), certain::kNetWorkError);
  ASSERT_EQ(epoll_io.RemoveAndCloseFd(fd_obj1), certain::kNetWorkError);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
