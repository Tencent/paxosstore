#ifndef CERTAIN_UTILS_HEADER_H_
#define CERTAIN_UTILS_HEADER_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <sys/stat.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <sched.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <signal.h>
#include <unistd.h>

#include <set>
#include <map>
#include <list>
#include <queue>
#include <string>
#include <iostream>
#include <vector>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <stack>

#include <tr1/memory>

using namespace std;

#endif // CERTAIN_HEADER_H_
