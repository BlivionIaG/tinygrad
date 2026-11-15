#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <dispatch/dispatch.h>
#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/IOMessage.h>
#include <mach/mach.h>
#include <sys/stat.h>
#include <time.h>

// Protocol

enum {
  CMD_PROBE = 0,          // probe devices, returns count
  CMD_MAP_BAR = 1,        // map PCI BAR, returns size
  CMD_MAP_SYSMEM_FD = 2,  // alloc DMA memory, returns fd via SCM_RIGHTS
  CMD_CFG_READ = 3,       // read PCI config space
  CMD_CFG_WRITE = 4,      // write PCI config space
  CMD_RESET = 5,          // reset device
  CMD_MMIO_READ = 6,      // bulk read from BAR
  CMD_MMIO_WRITE = 7,     // bulk write to BAR
  CMD_MAP_SYSMEM = 8,     // map system memory
  CMD_SYSMEM_READ = 9,    // bulk read from system memory
  CMD_SYSMEM_WRITE = 10,  // bulk write to system memory
  CMD_RESIZE_BAR = 11,    // resize bar (noop)
  CMD_FEATURES = 18,      // what this server can do (see below): arg0 = the client's protocol version
  CMD_RING_SETUP = 19,    // shared-memory command ring: arg0 = bytes, the shm fd comes with the request
  CMD_RING_KICK = 20,     // wake the server out of its sleep (no answer)
  RESP_OK = 0, RESP_ERR = 1,
  FEAT_FEATURES = 1u << 0, // CMD_FEATURES itself
  FEAT_RING = 1u << 4,     // CMD_RING_SETUP / CMD_RING_KICK
  PROTO_VERSION = 1,
  // ring layout (the notes below), bytes
  RING_MAGIC_OFF = 0, RING_VERSION_OFF = 8, RING_REQ_HEAD = 128, RING_REQ_TAIL = 256, RING_RSP_HEAD = 384, RING_RSP_TAIL = 512, RING_SRV_STATE = 640,
  RING_HDR_BYTES = 16384, RING_MIN_CAP = 4096, RING_MAX_BYTES = 256 << 20,
};

// Features and the shared-memory command ring (additive: an old server answers (1, 0, 0) to 18, 19 and 20; protocol version 1).
//   FEATURES: arg0 = the client's protocol version. status 0, resp0 = 0 (reserved), resp1 = version << 32 | feature bits (FEAT_FEATURES, FEAT_RING; no other bit
//     is set by this server). An old server's answer has resp1 = 0: no features.
//   FEAT_RING: for a client that polls, MMIO requests need no syscall and no wake-up while the server is busy. The socket stays: everything but MMIO and config
//     requests, features, and EOF detection (the server frees everything when the connection closes).
//   CMD_RING_SETUP: arg0 = bytes, the fd of a POSIX shm object of at least that size comes with the request (SCM_RIGHTS), created by the client and zero-filled apart
//     from its static header (below). The server maps it, checks the header, closes the fd. Status 0: resp0 = bytes, resp1 = 1. Refused: status 1, resp1 = 1,
//     resp0 = a kern_return_t-like code (0xE00002C2 bad header / size, 0xE00002D5 a ring is already set up). One ring per connection, until it closes.
//     The client must not shrink the object while it is set up (the server would take a SIGBUS).
//   Layout (little endian, u64 words are the only shared words, each alone in a 128-byte line):
//     0 u64 magic RING_MAGIC | 8 u32 version = 1, u32 req_cap, u32 rsp_cap (bytes, powers of two >= RING_MIN_CAP), u32 data_off = RING_HDR_BYTES
//     128 req_head (client)  256 req_tail (server)  384 rsp_head (server)  512 rsp_tail (client)  640 srv_state (server; the client CASes 1 -> 2)
//     data_off: the request byte ring (req_cap bytes), then the response byte ring (rsp_cap bytes). All four counters are monotonic byte counts, a byte lives at
//     (count & (cap - 1)); a producer stores its head with release after the bytes, a consumer its tail with release after it read them, and each side loads
//     the other's word with acquire. The server copies the header once: whatever the client writes there later is never looked at again.
//   The request ring carries the socket's byte stream: 33-byte requests (MMIO_WRITE followed by its arg1 payload bytes) back to back, no padding; a request or a
//     payload may be longer than the ring (the consumer drains while the producer fills). The response ring carries the answers: a 17-byte response, and for a
//     successful MMIO_READ its resp0 bytes. Only MMIO_READ, MMIO_WRITE (posted, no answer), CFG_READ, CFG_WRITE, RESIZE_BAR and RESET are served from the ring;
//     anything else gets (1, 0, 0) there. A client that has a ring sends those commands ONLY there (the order between the two channels is not defined) and
//     everything else on the socket, one request at a time with its answer awaited; a socket request is served after the ring entries published before it.
//   Sleeping: the server polls the ring for RING_SPIN_NS after the last request, then stores srv_state = 1, fences, looks at the ring once more and blocks in
//     recvmsg. A client that published (req_head, then a full fence) and then finds srv_state == 1 CASes it to 2 and sends one CMD_RING_KICK request (33 bytes
//     like any request, no answer, nothing else on the socket is affected); the server drops it on waking.
//   Waiting: the client polls rsp_head; the server never sleeps while an answer is pending. If the client goes away the server sees EOF on the socket, serves what
//     is in the ring, then cleans up like for any disconnect. A server that has to wait for the client (the rest of a payload, room in the response ring) gives
//     up on a client that is gone, that moved no word for RING_STALL_NS, or whose counters make no sense (more than a ring ahead, or behind): it is dropped.

typedef struct { uint8_t cmd; uint32_t dev_id, bar; uint64_t arg0, arg1, arg2; } __attribute__((packed)) request_t;
typedef struct { uint8_t status; uint64_t resp0, resp1; } __attribute__((packed)) response_t;

// Constants and state

#define BULK_BUF_SIZE (64 << 20)
#define MAX_BARS 6
#define MAX_SYSMEM 128

#define RING_MAGIC 0x5447524E47000001ull // "TGRNG", version 1
#define RING_SPIN_NS 200000ull           // how long the server polls the ring after the last request before it sleeps
#define RING_STALL_NS 30000000000ull     // how long it waits for a client that does not move a ring word
#define KR_BAD_ARGUMENT 0xE00002C2u      // kIOReturnBadArgument
#define KR_BUSY         0xE00002D5u      // kIOReturnBusy

static uint8_t g_bulk_buf[BULK_BUF_SIZE];
static io_connect_t g_conn = IO_OBJECT_NULL;
static int g_client_active = 0;

static struct { mach_vm_address_t addr; mach_vm_size_t size; } g_bars[MAX_BARS];
static struct { mach_vm_address_t addr; mach_vm_size_t size; int shm_fd; char shm_name[32]; } g_sysmem[MAX_SYSMEM];
static int g_sysmem_count = 0;

// Utilities

static void recvall(int fd, void *buf, size_t len) {
  for (size_t off = 0; off < len; ) {
    ssize_t r = recv(fd, (uint8_t*)buf + off, len - off, 0);
    if (r <= 0) break;
    off += r;
  }
}

// MMIO requires 32-bit aligned volatile accesses
static void mmio_copy(void *dst, void *src, size_t len) {
  volatile uint32_t *d = dst, *s = src;
  for (size_t i = 0; i < len / 4; i++) d[i] = s[i];
  for (size_t i = len & ~3; i < len; i++) ((volatile uint8_t*)dst)[i] = ((volatile uint8_t*)src)[i];
}

// one request; an fd that came with it (SCM_RIGHTS) goes to *out_fd, else -1. 0 on a whole request, -1 on EOF or error.
static int recv_request(int fd, request_t *req, int *out_fd) {
  char cmsgbuf[CMSG_SPACE(sizeof(int))];
  struct iovec iov = {req, sizeof(*req)};
  struct msghdr msg = {.msg_iov = &iov, .msg_iovlen = 1, .msg_control = cmsgbuf, .msg_controllen = sizeof(cmsgbuf)};
  *out_fd = -1;
  ssize_t r = recvmsg(fd, &msg, 0);
  if (r <= 0) return -1;
  for (struct cmsghdr *c = CMSG_FIRSTHDR(&msg); c; c = CMSG_NXTHDR(&msg, c))
    if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SCM_RIGHTS) memcpy(out_fd, CMSG_DATA(c), sizeof(int));
  for (size_t off = r; off < sizeof(*req); off += r) { // a short read of the header
    r = recv(fd, (uint8_t*)req + off, sizeof(*req) - off, 0);
    if (r <= 0) { if (*out_fd >= 0) close(*out_fd); *out_fd = -1; return -1; }
  }
  return 0;
}

static int send_response(int fd, response_t *resp, int send_fd) {
  char cmsgbuf[CMSG_SPACE(sizeof(int))];
  struct iovec iov = {resp, sizeof(*resp)};
  struct msghdr msg = {.msg_iov = &iov, .msg_iovlen = 1};

  if (send_fd >= 0) {
    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    *cmsg = (struct cmsghdr){.cmsg_level = SOL_SOCKET, .cmsg_type = SCM_RIGHTS, .cmsg_len = CMSG_LEN(sizeof(int))};
    memcpy(CMSG_DATA(cmsg), &send_fd, sizeof(int));
  }
  return sendmsg(fd, &msg, 0) > 0 ? 0 : -1;
}

static void send_error(int fd, const char *msg) {
  response_t resp = {.status = RESP_ERR, .resp0 = strlen(msg)};
  send_response(fd, &resp, -1);
  send(fd, msg, strlen(msg), 0);
}

// Driver interface

static void on_disconnect(void *refcon, io_service_t svc, uint32_t msg, void *arg) {
  if (msg == kIOMessageServiceIsTerminated) _exit(0);
}

static io_connect_t open_tinygpu(void) {
  static io_object_t notif;
  io_service_t svc = IOServiceGetMatchingService(kIOMainPortDefault, IOServiceNameMatching("tinygpu"));
  if (!svc) return IO_OBJECT_NULL;

  if (!notif) {
    IONotificationPortRef port = IONotificationPortCreate(kIOMainPortDefault);
    IONotificationPortSetDispatchQueue(port, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0));
    IOServiceAddInterestNotification(port, svc, kIOGeneralInterest, on_disconnect, NULL, &notif);
  }

  io_connect_t conn;
  kern_return_t kr = IOServiceOpen(svc, mach_task_self(), 0, &conn);
  IOObjectRelease(svc);
  return kr == KERN_SUCCESS ? conn : IO_OBJECT_NULL;
}

static int dext_rpc(uint32_t sel, uint64_t *in, uint32_t in_cnt, uint64_t *out_val) {
  uint64_t out[2];
  uint32_t out_cnt = 2;
  if (IOConnectCallMethod(g_conn, sel, in, in_cnt, NULL, 0, out, &out_cnt, NULL, NULL) != KERN_SUCCESS) return -1;
  if (out_val) *out_val = out[0];
  return 0;
}

static int map_bar(uint32_t bar, response_t *resp) {
  if (bar >= MAX_BARS) return -1;
  if (!g_bars[bar].addr && IOConnectMapMemory64(g_conn, bar, mach_task_self(), &g_bars[bar].addr, &g_bars[bar].size, kIOMapAnywhere)) return -1;
  resp->resp0 = g_bars[bar].addr;
  resp->resp1 = g_bars[bar].size;
  return 0;
}

static int map_sysmem_fd(uint64_t size, int contiguous, response_t *resp, int *out_fd) {
  if (g_sysmem_count >= MAX_SYSMEM) return -1;
  int idx = g_sysmem_count;
  int fd = -1;
  void *ptr = MAP_FAILED;
  char shm_name[32];

  // page-align, min 16KB for IOMemoryDescriptor
  size_t alloc_sz = (size + 0xfff) & ~0xfff;
  if (alloc_sz < 0x4000) alloc_sz = 0x4000;

  snprintf(shm_name, sizeof(shm_name), "/tinygpu_%d", idx);
  shm_unlink(shm_name);
  if ((fd = shm_open(shm_name, O_CREAT | O_RDWR, 0600)) < 0) goto fail;
  if (ftruncate(fd, alloc_sz) < 0) goto fail;
  if ((ptr = mmap(NULL, alloc_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED) goto fail;

  // PrepareDMA writes physical addresses to output buffer, copy to shared mem
  uint8_t paddr_buf[8192] = {0};
  size_t out_sz = sizeof(paddr_buf);
  if (IOConnectCallStructMethod(g_conn, 3, ptr, alloc_sz, paddr_buf, &out_sz) != KERN_SUCCESS) goto fail;
  memcpy(ptr, paddr_buf, out_sz);

  g_sysmem[idx] = (typeof(g_sysmem[idx])){.addr = (mach_vm_address_t)ptr, .size = alloc_sz, .shm_fd = fd};
  strncpy(g_sysmem[idx].shm_name, shm_name, sizeof(g_sysmem[idx].shm_name));
  g_sysmem_count++;

  *resp = (response_t){.resp0 = alloc_sz, .resp1 = idx};
  *out_fd = fd;
  return 0;

fail:
  if (ptr != MAP_FAILED) munmap(ptr, alloc_sz);
  if (fd >= 0) close(fd);
  shm_unlink(shm_name);
  return -1;
}

static int validate_bar(uint32_t bar, uint64_t off, uint64_t sz) { // off and sz come from the client: off + sz must not wrap
  return (bar < MAX_BARS && g_bars[bar].addr && sz <= g_bars[bar].size && off <= g_bars[bar].size - sz && sz <= BULK_BUF_SIZE) ? 0 : -1;
}

// the commands whose answer is one response and nothing else, no fd: the same on the socket and on the ring. 1 if req->cmd is one of them.
static int simple_cmd(const request_t *req, response_t *resp) {
  switch (req->cmd) {
  case CMD_CFG_READ: {
    uint64_t in[2] = {req->arg0, req->arg1};
    resp->status = dext_rpc(0, in, 2, &resp->resp0) ? 1 : 0;
    return 1;
  }
  case CMD_CFG_WRITE: {
    uint64_t in[3] = {req->arg0, req->arg1, req->arg2};
    resp->status = dext_rpc(1, in, 3, NULL) ? 1 : 0;
    return 1;
  }
  case CMD_RESIZE_BAR: return 1;
  case CMD_RESET:
    resp->status = dext_rpc(2, NULL, 0, NULL) ? 1 : 0;
    return 1;
  }
  return 0;
}

// Shared-memory command ring (see the notes at the top). Single producer, single consumer in each direction; counters are monotonic u64 byte counts.
static struct {
  uint8_t *base; size_t bytes;
  uint64_t req_cap, rsp_cap;      // bytes, powers of two
  uint8_t *req, *rsp;             // the two byte rings
  uint64_t rq_tail, rs_head;      // our own copies of the words only this side writes
} g_ring;

typedef struct { unsigned spins; uint64_t since; } ring_wait_t; // one wait for the client to move a word (since = 0: it has not started)

#define RING_U64(off) ((uint64_t*)(g_ring.base + (off)))
static inline uint64_t ring_load(size_t off) { return __atomic_load_n(RING_U64(off), __ATOMIC_ACQUIRE); }
static inline void ring_store(size_t off, uint64_t v) { __atomic_store_n(RING_U64(off), v, __ATOMIC_RELEASE); }
static inline void cpu_relax(void) {
#if defined(__aarch64__)
  __asm__ volatile("yield");
#elif defined(__x86_64__)
  __asm__ volatile("pause");
#endif
}
static uint64_t now_ns(void) { return clock_gettime_nsec_np(CLOCK_UPTIME_RAW); }
static int is_pow2(uint64_t v) { return v && !(v & (v - 1)); }

static void ring_teardown(void) {
  if (g_ring.base) munmap(g_ring.base, g_ring.bytes);
  memset(&g_ring, 0, sizeof(g_ring));
}

// CMD_RING_SETUP: the client's shm object (fd) is mapped, its header checked. status 0: resp0 = bytes, resp1 = 1. Refused: status 1, resp1 = 1, resp0 = a kern_return_t-like code.
static int ring_setup(uint64_t bytes, int shm_fd, response_t *resp) {
  resp->resp1 = 1;
  uint64_t err = KR_BAD_ARGUMENT;
  struct stat st;
  uint8_t *m = MAP_FAILED;
  if (g_ring.base) { err = KR_BUSY; goto fail; }
  if (shm_fd < 0 || bytes < RING_HDR_BYTES + 2 * RING_MIN_CAP || bytes > RING_MAX_BYTES || fstat(shm_fd, &st) || (uint64_t)st.st_size < bytes) goto fail;
  if ((m = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0)) == MAP_FAILED) goto fail;
  uint64_t magic;
  uint32_t w[4];
  memcpy(&magic, m + RING_MAGIC_OFF, sizeof(magic));
  memcpy(w, m + RING_VERSION_OFF, sizeof(w)); // version, req_cap, rsp_cap, data_off: read once, the client may change them later
  uint64_t req_cap = w[1], rsp_cap = w[2];
  int ok = magic == RING_MAGIC && w[0] == 1 && w[3] == RING_HDR_BYTES && is_pow2(req_cap) && is_pow2(rsp_cap) && req_cap >= RING_MIN_CAP && rsp_cap >= RING_MIN_CAP
    && RING_HDR_BYTES + req_cap + rsp_cap <= bytes;
  for (size_t off = RING_REQ_HEAD; ok && off <= RING_SRV_STATE; off += 128) ok = *(uint64_t*)(m + off) == 0; // a fresh object
  if (!ok) goto fail;
  close(shm_fd);
  g_ring.base = m; g_ring.bytes = bytes; g_ring.req_cap = req_cap; g_ring.rsp_cap = rsp_cap;
  g_ring.req = m + RING_HDR_BYTES; g_ring.rsp = g_ring.req + req_cap;
  g_ring.rq_tail = g_ring.rs_head = 0;
  resp->resp0 = bytes;
  return 0;
fail:
  if (m != MAP_FAILED) munmap(m, bytes);
  if (shm_fd >= 0) close(shm_fd);
  resp->resp0 = err;
  return -1;
}

static inline uint64_t ring_avail(void) { return ring_load(RING_REQ_HEAD) - g_ring.rq_tail; }

// the client is gone (EOF or a hard socket error); data waiting on the socket (a kick, a request) is not
static int peer_gone(int fd) {
  char b;
  ssize_t r = recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
  return r == 0 || (r < 0 && errno != EAGAIN && errno != EINTR);
}

// one step of waiting for the client to move a ring word: spin, look at the socket and the clock now and then, sleep a little once it takes long.
// -1: the client is gone or has not moved anything for RING_STALL_NS.
static int ring_stall(int fd, ring_wait_t *w) {
  cpu_relax();
  if (!w->since) w->since = now_ns();
  if (++w->spins & 1023) return 0;
  if (peer_gone(fd) || now_ns() - w->since > RING_STALL_NS) return -1;
  if (w->spins > 8192) usleep(20);
  return 0;
}

// take n bytes (at most ring_avail()) off the request ring
static void ring_pull(void *dst, size_t n) {
  uint64_t t = g_ring.rq_tail, pos = t & (g_ring.req_cap - 1), first = g_ring.req_cap - pos < n ? g_ring.req_cap - pos : n;
  memcpy(dst, g_ring.req + pos, first);
  memcpy((uint8_t*)dst + first, g_ring.req, n - first);
  g_ring.rq_tail = t + n;
  ring_store(RING_REQ_TAIL, t + n);
}

// n bytes that may still be on their way (a payload longer than the ring arrives while it is consumed)
static int ring_pull_wait(int fd, void *dst, size_t n) {
  uint8_t *d = dst;
  ring_wait_t w = {0};
  while (n) {
    uint64_t avail = ring_avail();
    if (avail > g_ring.req_cap) return -1;
    if (!avail) { if (ring_stall(fd, &w)) return -1; continue; }
    size_t k = avail < n ? (size_t)avail : n;
    ring_pull(d, k); d += k; n -= k; w = (ring_wait_t){0};
  }
  return 0;
}

// one answer (header, then data) onto the response ring; waits for the client to make room when it does not fit
static int ring_push(int fd, const void *a, size_t na, const void *b, size_t nb) {
  const uint8_t *p[2] = {a, b};
  size_t n[2] = {na, nb};
  uint64_t h = g_ring.rs_head, cap = g_ring.rsp_cap;
  ring_wait_t w = {0};
  for (int i = 0; i < 2; i++) {
    while (n[i]) {
      uint64_t used = h - ring_load(RING_RSP_TAIL);
      if (used > cap) return -1; // a tail the client could not have written (behind the head by more than the ring, or ahead of it)
      uint64_t space = cap - used;
      if (!space) {
        if (h != g_ring.rs_head) { g_ring.rs_head = h; ring_store(RING_RSP_HEAD, h); } // show what is there so the client can read it and make room
        if (ring_stall(fd, &w)) return -1;
        continue;
      }
      size_t k = space < n[i] ? (size_t)space : n[i];
      uint64_t pos = h & (cap - 1), first = cap - pos < k ? cap - pos : k;
      memcpy(g_ring.rsp + pos, p[i], first);
      memcpy(g_ring.rsp, p[i] + first, k - first);
      h += k; p[i] += k; n[i] -= k; w = (ring_wait_t){0};
    }
  }
  g_ring.rs_head = h;
  ring_store(RING_RSP_HEAD, h);
  return 0;
}

// one request that came on the ring (its header is consumed). -1: the client violated the protocol or went away: drop it.
static int ring_handle(int fd, const request_t *req) {
  response_t resp = {0};
  switch (req->cmd) {
  case CMD_MMIO_WRITE:
    if (req->arg1 > BULK_BUF_SIZE) return -1; // nothing that follows can be told from the next request
    if (ring_pull_wait(fd, g_bulk_buf, req->arg1)) return -1;
    if (!validate_bar(req->bar, req->arg0, req->arg1)) mmio_copy((void*)(g_bars[req->bar].addr + req->arg0), g_bulk_buf, req->arg1);
    return 0; // posted: no answer
  case CMD_MMIO_READ:
    if (validate_bar(req->bar, req->arg0, req->arg1)) { resp.status = 1; break; }
    mmio_copy(g_bulk_buf, (void*)(g_bars[req->bar].addr + req->arg0), req->arg1);
    resp.resp0 = req->arg1;
    return ring_push(fd, &resp, sizeof(resp), g_bulk_buf, req->arg1);
  default:
    if (!simple_cmd(req, &resp)) resp.status = 1; // everything that needs the socket (fds, features, ring setup) is refused here, like an unknown command
  }
  return ring_push(fd, &resp, sizeof(resp), NULL, 0);
}

// every request that is on the ring now. Returns how many, -1 if the client is gone / misbehaved.
static int ring_drain(int fd) {
  int n = 0;
  for (;;) {
    uint64_t avail = ring_avail();
    if (avail > g_ring.req_cap) return -1; // the head is not a count the client could have written (more than a ring ahead of the tail, or behind it)
    if (avail < sizeof(request_t)) break;
    request_t req;
    ring_pull(&req, sizeof(req));
    if (ring_handle(fd, &req)) return -1;
    n++;
  }
  return n;
}

// a CMD_RING_KICK only wakes the server out of its sleep: it is dropped (with an fd someone attached to it)
static int dropped_kick(const request_t *req, int *req_fd) {
  if (req->cmd != CMD_RING_KICK) return 0;
  if (*req_fd >= 0) { close(*req_fd); *req_fd = -1; }
  return 1;
}

// With a ring the blocking recvmsg of the request loop becomes: serve the ring, poll it for RING_SPIN_NS after the last request, then announce the sleep
// (srv_state = 1) and block on the socket, where the client's kick (or any request) wakes us. 0: a socket request is in *req (the ring was drained before it);
// -1: EOF or an error (the ring was drained first, so the writes the client posted before it closed still land).
static int ring_service(int fd, request_t *req, int *req_fd) {
  uint64_t since = now_ns();
  for (;;) {
    int n = ring_drain(fd);
    if (n < 0) return -1;
    if (n > 0) { since = now_ns(); continue; } // busy: the socket is looked at once the ring is empty, a request on it is served after the ring entries before it
    char b;
    ssize_t r = recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
    if (r == 0 || (r < 0 && errno != EAGAIN && errno != EINTR)) { ring_drain(fd); return -1; }
    if (r > 0) { // a kick or a request: the ring entries published before it are behind us
      if (ring_drain(fd) < 0 || recv_request(fd, req, req_fd)) return -1;
      if (dropped_kick(req, req_fd)) continue;
      return 0;
    }
    for (int k = 0; k < 64 && !ring_avail(); k++) cpu_relax();
    if (ring_avail() || now_ns() - since < RING_SPIN_NS) continue;
    __atomic_store_n(RING_U64(RING_SRV_STATE), 1, __ATOMIC_SEQ_CST); // Dekker: announce the sleep, fence, look once more
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    if (ring_avail()) { __atomic_store_n(RING_U64(RING_SRV_STATE), 0, __ATOMIC_SEQ_CST); continue; }
    int rr = recv_request(fd, req, req_fd);
    __atomic_store_n(RING_U64(RING_SRV_STATE), 0, __ATOMIC_SEQ_CST);
    if (rr) { ring_drain(fd); return -1; }
    if (dropped_kick(req, req_fd)) continue;
    if (ring_drain(fd) < 0) return -1;
    return 0;
  }
}

// the next request that needs the socket
static int next_request(int fd, request_t *req, int *req_fd) {
  return g_ring.base ? ring_service(fd, req, req_fd) : recv_request(fd, req, req_fd);
}

static void cleanup(void) {
  ring_teardown();
  for (int i = 0; i < MAX_BARS; i++)
    if (g_bars[i].addr) { IOConnectUnmapMemory64(g_conn, i, mach_task_self(), g_bars[i].addr); g_bars[i].addr = 0; }

  for (int i = 0; i < g_sysmem_count; i++) {
    munmap((void*)g_sysmem[i].addr, g_sysmem[i].size);
    close(g_sysmem[i].shm_fd);
    shm_unlink(g_sysmem[i].shm_name);
  }

  g_sysmem_count = 0;
  if (g_conn != IO_OBJECT_NULL) { IOServiceClose(g_conn); g_conn = IO_OBJECT_NULL; }
}

static void handle_client(int fd) {
  int bufsize = BULK_BUF_SIZE;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
  printf("client connected\n");

  g_conn = open_tinygpu();
  if (g_conn == IO_OBJECT_NULL) {
    fprintf(stderr, "failed to connect to tinygpu driver\n");
    request_t req; recv(fd, &req, sizeof(req), 0);
    send_error(fd, "Driver not available. Check: System Report > PCI for GPU, System Settings > Privacy & Security.");
    return;
  }

  request_t req;
  response_t resp;
  int req_fd;
  while (next_request(fd, &req, &req_fd) == 0) {
    resp = (response_t){0};
    if (req_fd >= 0 && req.cmd != CMD_RING_SETUP) { close(req_fd); req_fd = -1; }

    switch (req.cmd) {
    case CMD_MAP_BAR:
      resp.status = map_bar(req.bar, &resp) ? 1 : 0;
      break;

    case CMD_MAP_SYSMEM_FD: {
      int shm_fd = -1;
      resp.status = map_sysmem_fd(req.arg0, (int)req.arg1, &resp, &shm_fd) ? 1 : 0;
      send_response(fd, &resp, shm_fd);
      continue;
    }

    case CMD_FEATURES:
      resp.resp1 = (uint64_t)PROTO_VERSION << 32 | FEAT_FEATURES | FEAT_RING;
      break;

    case CMD_RING_SETUP:
      resp.status = ring_setup(req.arg0, req_fd, &resp) ? 1 : 0;
      break;

    case CMD_RING_KICK: // a wake-up that outlived its sleep: no answer
      continue;

    case CMD_MMIO_READ:
      if (validate_bar(req.bar, req.arg0, req.arg1)) { resp.status = 1; break; }
      mmio_copy(g_bulk_buf, (void*)(g_bars[req.bar].addr + req.arg0), req.arg1);
      resp.resp0 = req.arg1;
      send_response(fd, &resp, -1);
      send(fd, g_bulk_buf, req.arg1, 0);
      continue;

    case CMD_MMIO_WRITE:
      recvall(fd, g_bulk_buf, req.arg1);
      if (!validate_bar(req.bar, req.arg0, req.arg1))
        mmio_copy((void*)(g_bars[req.bar].addr + req.arg0), g_bulk_buf, req.arg1);
      continue;

    default:
      if (!simple_cmd(&req, &resp)) resp.status = 1;
    }
    send_response(fd, &resp, -1);
  }

  printf("client disconnected\n");
  cleanup();
}

int run_server(const char *sock_path) {
  int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_fd < 0) { perror("socket"); return 1; }

  struct sockaddr_un addr = {.sun_family = AF_UNIX};
  strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);
  unlink(sock_path);

  if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); close(server_fd); return 1; }
  if (listen(server_fd, 1) < 0) { perror("listen"); close(server_fd); return 1; }
  printf("listening on %s\n", sock_path);

  while (1) {
    int client_fd = accept(server_fd, NULL, NULL);
    if (client_fd < 0) { if (errno == EINTR) continue; perror("accept"); break; }
    if (g_client_active) { printf("rejected: client already connected\n"); close(client_fd); continue; }
    g_client_active = 1;
    handle_client(client_fd);
    g_client_active = 0;
    close(client_fd);
  }

  close(server_fd);
  unlink(sock_path);
  cleanup();
  return 0;
}
