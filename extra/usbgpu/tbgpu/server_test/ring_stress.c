// A client of the shared-memory command ring (server.c CMD_RING_SETUP), written the way a real one has to be: acquire/release on the ring words, a full fence between
// publishing and looking at srv_state, a CAS 1 -> 2 and one CMD_RING_KICK per sleep. Used by test_server.py to hammer the server's sleep / wake handshake
// (the server runs with a tiny TINYGPU_SPIN_US so it really sleeps between bursts) and the ordering of posted writes and reads.
//   ring_stress <socket> <iterations> <seed> [req_cap rsp_cap]
// Every iteration does a random thing to BAR0 of the mock (1 MiB of memory in the server): a posted write, a read that must return the model's bytes, a burst of
// posted writes followed by a read, a pause (so the server goes to sleep), or a write larger than the ring. At the end it posts writes and closes the socket without
// waiting: the server must apply them before it goes away; a second connection (made by the caller) checks that. Exit status 0 = every read matched.
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>

typedef struct { uint8_t cmd; uint32_t dev_id, bar; uint64_t arg0, arg1, arg2; } __attribute__((packed)) request_t;
typedef struct { uint8_t status; uint64_t resp0, resp1; } __attribute__((packed)) response_t;
enum { CFG_READ = 3, MMIO_READ = 6, MMIO_WRITE = 7, MAP_BAR = 1, RING_SETUP = 19, RING_KICK = 20, HDR = 16384,
       REQ_HEAD = 128, REQ_TAIL = 256, RSP_HEAD = 384, RSP_TAIL = 512, SRV_STATE = 640 };
#define MAGIC 0x5447524E47000001ull

static int sock;
static uint8_t *base, *reqr, *rspr;
static uint64_t req_cap, rsp_cap, rq_head, rs_tail, kicks, sleeps_seen;
#define W(off) ((uint64_t*)(base + (off)))

static void die(const char *m) { fprintf(stderr, "ring_stress: %s\n", m); exit(1); }
static uint64_t now_ns(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec * 1000000000ull + t.tv_nsec; }
static void deadline(uint64_t t0, const char *m) { if (now_ns() - t0 > 20000000000ull) die(m); }

static void sock_rpc(const request_t *r, response_t *out, int fd) {
  char cm[CMSG_SPACE(sizeof(int))];
  struct iovec iov = {(void*)r, sizeof(*r)};
  struct msghdr msg = {.msg_iov = &iov, .msg_iovlen = 1};
  if (fd >= 0) {
    msg.msg_control = cm; msg.msg_controllen = sizeof(cm);
    struct cmsghdr *c = CMSG_FIRSTHDR(&msg);
    *c = (struct cmsghdr){.cmsg_level = SOL_SOCKET, .cmsg_type = SCM_RIGHTS, .cmsg_len = CMSG_LEN(sizeof(int))};
    memcpy(CMSG_DATA(c), &fd, sizeof(int));
  }
  if (sendmsg(sock, &msg, 0) != (ssize_t)sizeof(*r)) die("socket send");
  if (!out) return;
  for (size_t got = 0; got < sizeof(*out);) { ssize_t n = recv(sock, (uint8_t*)out + got, sizeof(*out) - got, 0); if (n <= 0) die("socket recv"); got += n; }
}

// publish what was written, then the Dekker half of the sleep handshake
static void publish(uint64_t h) {
  rq_head = h;
  __atomic_store_n(W(REQ_HEAD), h, __ATOMIC_RELEASE);
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
  uint64_t one = 1;
  if (__atomic_load_n(W(SRV_STATE), __ATOMIC_SEQ_CST) == 1 && __atomic_compare_exchange_n(W(SRV_STATE), &one, 2, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
    request_t k = {.cmd = RING_KICK};
    sock_rpc(&k, NULL, -1);
    kicks++;
  }
}

static void req_put(const void *src, size_t n) {
  const uint8_t *s = src;
  uint64_t h = rq_head, t0 = now_ns();
  while (n) {
    uint64_t space = req_cap - (h - __atomic_load_n(W(REQ_TAIL), __ATOMIC_ACQUIRE));
    if (!space) { if (h != rq_head) publish(h); deadline(t0, "request ring stuck"); continue; }
    size_t k = space < n ? space : n, pos = h & (req_cap - 1), first = req_cap - pos < k ? req_cap - pos : k;
    memcpy(reqr + pos, s, first); memcpy(reqr, s + first, k - first);
    h += k; s += k; n -= k; t0 = now_ns();
  }
  publish(h);
}

static void rsp_get(void *dst, size_t n) {
  uint8_t *d = dst;
  uint64_t t0 = now_ns();
  while (n) {
    uint64_t avail = __atomic_load_n(W(RSP_HEAD), __ATOMIC_ACQUIRE) - rs_tail;
    if (!avail) { deadline(t0, "no answer on the response ring"); continue; }
    size_t k = avail < n ? avail : n, pos = rs_tail & (rsp_cap - 1), first = rsp_cap - pos < k ? rsp_cap - pos : k;
    memcpy(d, rspr + pos, first); memcpy(d + first, rspr, k - first);
    rs_tail += k; d += k; n -= k; t0 = now_ns();
    __atomic_store_n(W(RSP_TAIL), rs_tail, __ATOMIC_RELEASE);
  }
}

static void ring_write(uint64_t off, const void *data, size_t n) {
  request_t r = {.cmd = MMIO_WRITE, .bar = 0, .arg0 = off, .arg1 = n};
  uint8_t *buf = malloc(sizeof(r) + n);
  memcpy(buf, &r, sizeof(r)); memcpy(buf + sizeof(r), data, n);
  req_put(buf, sizeof(r) + n);  // one publish for header + payload
  free(buf);
}

static void ring_read(uint64_t off, void *dst, size_t n) {
  request_t r = {.cmd = MMIO_READ, .bar = 0, .arg0 = off, .arg1 = n};
  req_put(&r, sizeof(r));
  response_t resp;
  rsp_get(&resp, sizeof(resp));
  if (resp.status != 0 || resp.resp0 != n) die("read refused");
  rsp_get(dst, n);
}

#define KNOWN (1 << 17)  // the part of the BAR the model follows (writes longer than the ring reach past 64 KiB)
#define MODEL (1 << 20)
static uint8_t model[MODEL];
static uint64_t rng;
static uint32_t rnd(void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return (uint32_t)(rng >> 16); }

static void check_read(uint64_t off, size_t n) {
  uint8_t *got = malloc(n);
  ring_read(off, got, n);
  if (memcmp(got, model + off, n)) { fprintf(stderr, "ring_stress: mismatch at %llu len %zu\n", (unsigned long long)off, n); exit(1); }
  free(got);
}

static void do_write(uint64_t off, size_t n) {
  uint8_t *buf = malloc(n);
  for (size_t i = 0; i < n; i++) buf[i] = rnd();
  memcpy(model + off, buf, n);
  ring_write(off, buf, n);
  free(buf);
}

int main(int argc, char **argv) {
  if (argc < 4) die("usage: ring_stress <socket> <iterations> <seed> [req_cap rsp_cap]");
  long iters = atol(argv[2]);
  rng = strtoull(argv[3], NULL, 0) * 0x9E3779B97F4A7C15ull | 1;
  req_cap = argc > 4 ? strtoull(argv[4], NULL, 0) : 1 << 16;
  rsp_cap = argc > 5 ? strtoull(argv[5], NULL, 0) : 1 << 16;
  sock = socket(AF_UNIX, SOCK_STREAM, 0);
  struct sockaddr_un a = {.sun_family = AF_UNIX};
  strncpy(a.sun_path, argv[1], sizeof(a.sun_path) - 1);
  for (int i = 0; connect(sock, (struct sockaddr*)&a, sizeof(a)); i++) { if (i > 500) die("no server"); usleep(20000); }
  request_t cfg = {.cmd = CFG_READ, .arg0 = 0, .arg1 = 2};
  response_t resp;
  sock_rpc(&cfg, &resp, -1);
  request_t mb = {.cmd = MAP_BAR, .bar = 0};
  sock_rpc(&mb, &resp, -1);
  if (resp.status) die("map bar");

  size_t bytes = HDR + req_cap + rsp_cap;
  char name[64];
  snprintf(name, sizeof(name), "/tgring%d", getpid());
  int fd = shm_open(name, O_CREAT | O_RDWR, 0600);
  if (fd < 0) die("shm_open");
  shm_unlink(name);
  if (ftruncate(fd, bytes)) die("ftruncate");
  base = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED) die("mmap");
  uint64_t magic = MAGIC; uint32_t w[4] = {1, req_cap, rsp_cap, HDR};
  memcpy(base, &magic, 8); memcpy(base + 8, w, 16);
  reqr = base + HDR; rspr = reqr + req_cap;
  request_t setup = {.cmd = RING_SETUP, .arg0 = bytes};
  sock_rpc(&setup, &resp, fd);
  if (resp.status || resp.resp0 != bytes) die("ring setup refused");
  close(fd);

  // the BAR starts as the previous run left it: learn it through the ring
  ring_read(0, model, KNOWN);
  for (long it = 0; it < iters; it++) {
    uint32_t op = rnd() % 100;
    uint64_t off = (rnd() % ((1 << 16) / 4)) * 4;
    if (op < 35) { do_write(off, 4 * (1 + rnd() % 16)); }
    else if (op < 65) { size_t n = 4 * (1 + rnd() % 64); if (off + n > (1 << 16)) off = 0; check_read(off, n); }
    else if (op < 85) { int k = 1 + rnd() % 20; for (int i = 0; i < k; i++) do_write((rnd() % ((1 << 16) / 4 - 32)) * 4, 4 * (1 + rnd() % 8)); check_read(0, 1 << 12); }
    else if (op < 93) { usleep(rnd() % 400); } // the server sleeps (it polls for TINYGPU_SPIN_US only)
    else if (op < 96) { do_write(0, req_cap + 4 * (rnd() % 1024)); } // longer than the ring, goes through in pieces
    else if (op < 99) { size_t n = rsp_cap + 4 * (rnd() % 1024); check_read(0, n); } // an answer longer than the ring
    else { check_read(0, 1 << 16); }
  }
  check_read(0, 1 << 16);
  // EOF with entries pending: post writes, close, do not look back
  for (int i = 0; i < 50; i++) do_write((rnd() % ((1 << 16) / 4 - 32)) * 4, 4 * (1 + rnd() % 8));
  FILE *f = fopen(argc > 6 ? argv[6] : "/dev/null", "w");
  if (f) { fwrite(model, 1, KNOWN, f); fclose(f); }
  printf("ok kicks=%llu\n", (unsigned long long)kicks);
  close(sock);
  return 0;
}
