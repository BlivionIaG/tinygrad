// Included (-include) only into server.c of the test build: counts the sendmsg / send calls the server makes, for the one-send MMIO_READ answer (CfgRead 0xF00C).
#include <sys/types.h>
#include <sys/socket.h>
extern unsigned long mock_sends;
ssize_t mock_sendmsg(int fd, const struct msghdr *msg, int flags);
ssize_t mock_send(int fd, const void *buf, size_t len, int flags);
#define sendmsg mock_sendmsg
#define send mock_send
