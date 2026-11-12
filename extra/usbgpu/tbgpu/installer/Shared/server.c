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
#include <pthread.h>
#include <poll.h>
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
  CMD_UNMAP_SYSMEM = 15,  // give a DMA block back: arg0 = block index (resp1 of the call that made it)
  CMD_MAP_FD = 16,        // DMA map a range of client memory: fd via SCM_RIGHTS, arg0 = offset, arg1 = size (multiples of 16 KiB)
  CMD_RESET_LINK = 17,    // reset the PCIe link: arg0 = level, arg1 = type (0: query), arg2 = options
  CMD_FEATURES = 18,      // what this server can do (see below): arg0 = the client's protocol version
  RESP_OK = 0, RESP_ERR = 1,
  FEAT_FEATURES = 1u << 0,   // CMD_FEATURES itself
  FEAT_READ_1SEND = 1u << 1, // an MMIO_READ answer (header + data) leaves in one sendmsg
  FEAT_WC_BAR0 = 1u << 2,    // the dext maps a BAR write-combined for the user-client memory type WC_BAR_TYPE | bar
  FEAT_QOS_SPIN = 1u << 3,   // the serving thread runs at QOS_CLASS_USER_INTERACTIVE and honours TINYGPU_SPIN_US
  PROTO_VERSION = 1,
  WC_BAR_TYPE = 0xF0000100u, // IOConnectMapMemory64 type of a write-combined BAR: WC_BAR_TYPE | bar (plain types stay `bar`; DMA types are sizes < 2047 MiB)
};

// Features and the fast path (additive: an old server answers (1, 0, 0) to 18, a new one sets resp1 != 0). The client sends its protocol version in arg0.
//   FEATURES: status 0, resp0 = the IORegistryEntryID of the `tinygpu` service (0 = unknown; a second process can find the very service with
//     IORegistryEntryIDMatching), resp1 = version << 32 | feature bits. An old server's answer has resp1 = 0: no features, find the service by name.
//   FEAT_READ_1SEND: the answer to MMIO_READ is the same byte stream as always (a 17-byte response, then the data) but leaves in one sendmsg: one send and
//     one wake-up less on each side. Needs nothing from the client.
//   FEAT_WC_BAR0: the dext answered selector 6 (Features) with bit 0: a user client of the dext can map BAR b write-combined with IOConnectMapMemory64
//     type WC_BAR_TYPE | b (bulk BAR0 stores; BAR2 doorbells and BAR5 registers keep the plain type). Only a process that may open the dext itself can use it.
//   FEAT_QOS_SPIN: the serving thread runs at QOS_CLASS_USER_INTERACTIVE. TINYGPU_SPIN_US=n (default 0) makes the server poll its socket for n microseconds
//     after each answer before it blocks, which saves the wake-up of a dense request stream (a request after a longer pause still finds it asleep).

typedef struct { uint8_t cmd; uint32_t dev_id, bar; uint64_t arg0, arg1, arg2; } __attribute__((packed)) request_t;
typedef struct { uint8_t status; uint64_t resp0, resp1; } __attribute__((packed)) response_t;

// Releasing DMA memory (all additive: an old client never sends 15 or 16, an old server answers (1, 0, 0) to them). Needs a dext with ReleaseDMA.
// A block is a live MAP_SYSMEM_FD / MAP_FD answer; its index (resp1 of that answer) is the lowest free slot, at most MAX_SYSMEM are live, and it is reused.
//   UNMAP_SYSMEM: status 0, resp0 = bytes released, resp1 = 1. Refused: status 1, resp1 = 1, resp0 = the kern_return_t (0xE00002C2 no such block,
//     0xE00002C7 dext without ReleaseDMA); the block stays live. Status 1 with resp1 = 0 is an old server: the client should not ask again.
//   MAP_FD: the fd of a POSIX shm object the client owns comes with the request (SCM_RIGHTS), nothing is written into it. Response: status 0,
//     resp0 = bytes mapped, resp1 = block index, then a payload: u64 nseg, nseg (iova, length) pairs. Failure: status 1, no payload.
// UNMAP removes the IOMMU mapping, a device that touches the range afterwards faults (an AMD SDMA engine may hang until a power cycle). The server
// cannot check: the client must first idle every engine that can reach the block and remove its own device mappings (GPU PTEs and TLB flush).
// The total live DMA is still capped by the aperture of the Thunderbolt PCIe controller's DART (about 2047 MiB): this time-multiplexes it.

// Link reset (additive: an old server answers (1, 0, 0) to 17, a new one always sets resp1 = 1). A dead Thunderbolt / PCIe link can be reset without a replug.
// IOPCIDevice::Reset(type, options) on a nub toggles the secondary bus reset of that nub's PARENT bridge, so it resets everything below that bridge.
//   level 0: the GPU's nub (dext selector 5 of the GPU user client). level k >= 1: the k-th IOPCIDevice above the GPU nub that has a `tinygpubridge` dext
//   child (the TinyGPUBridge personality of the dext, only bound to bridge nubs when the dext holds the bridge entitlement), reached through a user client
//   opened for this request only and closed right after it, nothing is held between requests.
//   type = 0 is a query, nothing is reset: status 0, resp0 = number of levels above the GPU the server can see (valid levels are 0..resp0), resp1 = 1.
//   type != 0 (tIOPCIDeviceResetTypes: 1 hot reset, 2 warm, 4 warm disable, 8 warm enable, 0x10 function), options (1: terminate the nubs below the bridge and
//     re-probe the bus): status 0, resp0 = 0, resp1 = 1. Failed: status 1, resp1 = 1, resp0 = the kern_return_t (0xE00002C0 no bridge dext at that level,
//     0xE00002C7 a GPU dext without selector 5, the IOServiceOpen error if the bridge user client does not open, else what Reset returned).
// Success does not prove the device came back: IOPCIDevice::Reset ignores the result of its wait for the link. With options & 1 the dext terminates
// asynchronously and this server exits after it answered (the client sees the connection close).

// Constants and state

#define BULK_BUF_SIZE (64 << 20)
#define MAX_BARS 6
#define MAX_SYSMEM 128           // live DMA blocks per client
#define MAP_FD_ALIGN (16 << 10)
#define MAX_SEGMENTS 32          // the dext's IODMACommand answer
#define KR_NO_DEVICE    0xE00002C0u   // kIOReturnNoDevice
#define KR_BAD_ARGUMENT 0xE00002C2u   // kIOReturnBadArgument
#define KR_UNSUPPORTED  0xE00002C7u   // kIOReturnUnsupported
#define RELEASE_MAGIC 0x52454C444D410001ull // the dext appends (RELEASE_MAGIC, slot) to its address table when it has ReleaseDMA
#define MAX_REGISTRY_HOPS 32          // parents followed from the tinygpu service when looking for bridge dexts
#define BRIDGE_DEXT_NAME "tinygpubridge"

static uint8_t g_bulk_buf[BULK_BUF_SIZE];
static io_connect_t g_conn = IO_OBJECT_NULL;
static int g_client_active = 0;
static uint64_t g_reg_id;       // IORegistryEntryID of the tinygpu service (0 = unknown)
static uint64_t g_spin_ns;      // TINYGPU_SPIN_US in ns
static volatile int g_terminating = 0, g_exit_pending = 0; // a link reset with the terminate option is in flight / the dext went away meanwhile

static struct { mach_vm_address_t addr; mach_vm_size_t size; } g_bars[MAX_BARS];
// a live block: made by MAP_SYSMEM_FD (shm_fd >= 0, the server owns the shm object) or MAP_FD (shm_fd = -1, the client owns the memory)
static struct { mach_vm_address_t addr; mach_vm_size_t size; int live, shm_fd; int64_t dext_slot; char shm_name[32]; } g_sysmem[MAX_SYSMEM];

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

// the answer of one request and its data in ONE sendmsg (the byte stream is the one a send_response followed by a send makes)
static int send_response_data(int fd, response_t *resp, const void *data, size_t len) {
  struct iovec iov[2] = {{resp, sizeof(*resp)}, {(void*)data, len}};
  struct msghdr msg = {.msg_iov = iov, .msg_iovlen = 2};
  size_t left = sizeof(*resp) + len;
  while (left) {
    ssize_t r = sendmsg(fd, &msg, 0);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return -1;
    left -= r;
    if (!left) break;
    size_t skip = r; // a partial send: step over what went out
    struct iovec *v = msg.msg_iov;
    while (skip >= v->iov_len) { skip -= v->iov_len; v++; msg.msg_iovlen--; }
    v->iov_base = (uint8_t*)v->iov_base + skip;
    v->iov_len -= skip;
    msg.msg_iov = v;
  }
  return 0;
}

static uint64_t now_ns(void) { return clock_gettime_nsec_np(CLOCK_UPTIME_RAW); }

// TINYGPU_SPIN_US: after an answer, poll the socket for this long before blocking in recvmsg (a request that arrives meanwhile is picked up without a wake-up)
static void spin_for_request(int fd) {
  if (!g_spin_ns) return;
  uint64_t since = now_ns();
  for (unsigned i = 0;; i++) {
    char b;
    ssize_t r = recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
    if (r >= 0 || (errno != EAGAIN && errno != EINTR)) return; // data, EOF or an error: recv_request deals with it
    if ((i & 15) == 15 && now_ns() - since > g_spin_ns) return;
  }
}

// Driver interface

static void on_disconnect(void *refcon, io_service_t svc, uint32_t msg, void *arg) {
  if (msg != kIOMessageServiceIsTerminated) return;
  __atomic_store_n(&g_exit_pending, 1, __ATOMIC_SEQ_CST);
  if (!__atomic_load_n(&g_terminating, __ATOMIC_SEQ_CST)) _exit(0); // else end_link_reset exits once the answer of the reset that terminates the dext went out
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
  uint64_t id = 0;
  g_reg_id = IORegistryEntryGetRegistryEntryID(svc, &id) == KERN_SUCCESS ? id : 0;
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

// selector 6 of the dext's user client (Features): bit 0 = it maps a BAR write-combined for the type WC_BAR_TYPE | bar. A dext without it answers kIOReturnUnsupported.
static int dext_has_wc(void) {
  uint64_t v = 0;
  return dext_rpc(6, NULL, 0, &v) == 0 && (v & 1);
}

static uint32_t dext_call(uint32_t sel, uint64_t *in, uint32_t in_cnt) {
  uint64_t out[2];
  uint32_t out_cnt = 2;
  return IOConnectCallMethod(g_conn, sel, in, in_cnt, NULL, 0, out, &out_cnt, NULL, NULL);
}

static int map_bar(uint32_t bar, response_t *resp) {
  if (bar >= MAX_BARS) return -1;
  if (!g_bars[bar].addr && IOConnectMapMemory64(g_conn, bar, mach_task_self(), &g_bars[bar].addr, &g_bars[bar].size, kIOMapAnywhere)) return -1;
  resp->resp0 = g_bars[bar].addr;
  resp->resp1 = g_bars[bar].size;
  return 0;
}

static int free_slot(void) {
  for (int i = 0; i < MAX_SYSMEM; i++) if (!g_sysmem[i].live) return i;
  return -1;
}

// PrepareDMA fills the table with (address, length) pairs ended by (0, 0); a dext with ReleaseDMA adds (RELEASE_MAGIC, its slot number)
static int64_t table_dext_slot(const uint64_t *tab, size_t n, uint32_t *nseg) {
  size_t i = 0;
  while (i + 1 < n && (tab[i] || tab[i + 1])) i += 2;
  if (nseg) *nseg = (uint32_t)(i / 2);
  return (i + 3 < n && tab[i + 2] == RELEASE_MAGIC) ? (int64_t)tab[i + 3] : -1;
}

static int map_sysmem_fd(uint64_t size, int contiguous, response_t *resp, int *out_fd) {
  int idx = free_slot();
  if (idx < 0) return -1;
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
  int64_t dext_slot = table_dext_slot((uint64_t*)paddr_buf, out_sz / 8, NULL);
  memcpy(ptr, paddr_buf, out_sz);

  g_sysmem[idx] = (typeof(g_sysmem[idx])){.addr = (mach_vm_address_t)ptr, .size = alloc_sz, .live = 1, .shm_fd = fd, .dext_slot = dext_slot};
  strncpy(g_sysmem[idx].shm_name, shm_name, sizeof(g_sysmem[idx].shm_name));

  *resp = (response_t){.resp0 = alloc_sz, .resp1 = idx};
  *out_fd = fd;
  return 0;

fail:
  if (ptr != MAP_FAILED) munmap(ptr, alloc_sz);
  if (fd >= 0) close(fd);
  shm_unlink(shm_name);
  return -1;
}

// DMA map [off, off + size) of a client's shm object: nothing is written into it, the address table goes to the client in the response
static int map_fd(int cfd, uint64_t off, uint64_t size, response_t *resp, uint64_t *tab, uint32_t *nseg) { // tab: 1 + 2 * MAX_SEGMENTS words
  int idx = free_slot();
  if (cfd < 0 || idx < 0 || !size || (off | size) % MAP_FD_ALIGN) return -1;
  void *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, cfd, off);
  if (ptr == MAP_FAILED) return -1;

  uint8_t paddr_buf[8192] = {0};
  size_t out_sz = sizeof(paddr_buf);
  if (IOConnectCallStructMethod(g_conn, 3, ptr, size, paddr_buf, &out_sz) != KERN_SUCCESS) { munmap(ptr, size); return -1; }
  int64_t dext_slot = table_dext_slot((uint64_t*)paddr_buf, out_sz / 8, nseg);
  if (*nseg > MAX_SEGMENTS) *nseg = MAX_SEGMENTS;
  tab[0] = *nseg;
  memcpy(tab + 1, paddr_buf, *nseg * 16);

  g_sysmem[idx] = (typeof(g_sysmem[idx])){.addr = (mach_vm_address_t)ptr, .size = size, .live = 1, .shm_fd = -1, .dext_slot = dext_slot};
  *resp = (response_t){.resp0 = size, .resp1 = idx};
  return 0;
}

// status 0: released; status 1 with resp1 = 1: understood but refused, resp0 = kern_return_t. A server without this command answers status 1 with resp1 = 0.
static void unmap_sysmem(uint64_t idx, response_t *resp) {
  resp->resp1 = 1;
  if (idx >= MAX_SYSMEM || !g_sysmem[idx].live) { resp->status = 1; resp->resp0 = KR_BAD_ARGUMENT; return; }
  if (g_sysmem[idx].dext_slot < 0) { resp->status = 1; resp->resp0 = KR_UNSUPPORTED; return; }
  uint64_t slot = g_sysmem[idx].dext_slot;
  uint32_t kr = dext_call(4, &slot, 1);
  if (kr != KERN_SUCCESS) { resp->status = 1; resp->resp0 = kr; return; } // still mapped, still live
  munmap((void*)g_sysmem[idx].addr, g_sysmem[idx].size);
  if (g_sysmem[idx].shm_fd >= 0) { close(g_sysmem[idx].shm_fd); shm_unlink(g_sysmem[idx].shm_name); }
  resp->resp0 = g_sysmem[idx].size;
  memset(&g_sysmem[idx], 0, sizeof(g_sysmem[idx]));
}

// The `tinygpubridge` service (a TinyGPUBridge dext) of the level-th IOPCIDevice above the GPU's nub that has one, level >= 1; IO_OBJECT_NULL if there is none.
// Registry walk: from the tinygpu service up the service plane (the parent of the dext is the GPU's nub, the parent of a nub is the IOPCI2PCIBridge driver of
// its parent bridge, whose parent is the next nub up), no DriverKit API reaches an ancestor nub, only a dext bound to it can reset it. The caller releases it.
static io_service_t find_bridge_dext(uint32_t level) {
  io_registry_entry_t e = IOServiceGetMatchingService(kIOMainPortDefault, IOServiceNameMatching("tinygpu"));
  uint32_t nubs = 0, levels = 0; // IOPCIDevices passed (the first is the GPU's nub, level 0), nubs with a bridge dext passed
  for (int hop = 0; e != IO_OBJECT_NULL && hop < MAX_REGISTRY_HOPS; hop++) {
    io_registry_entry_t parent = IO_OBJECT_NULL;
    kern_return_t kr = IORegistryEntryGetParentEntry(e, kIOServicePlane, &parent);
    IOObjectRelease(e);
    e = kr == KERN_SUCCESS ? parent : IO_OBJECT_NULL;
    if (e == IO_OBJECT_NULL || !IOObjectConformsTo(e, "IOPCIDevice") || nubs++ == 0) continue;

    io_iterator_t it;
    if (IORegistryEntryGetChildIterator(e, kIOServicePlane, &it) != KERN_SUCCESS) continue;
    io_service_t found = IO_OBJECT_NULL;
    for (io_object_t child; found == IO_OBJECT_NULL && (child = IOIteratorNext(it)) != IO_OBJECT_NULL; ) {
      io_name_t name;
      if (IORegistryEntryGetName(child, name) == KERN_SUCCESS && !strcmp(name, BRIDGE_DEXT_NAME) && ++levels == level) found = child;
      else IOObjectRelease(child);
    }
    IOObjectRelease(it);
    if (found != IO_OBJECT_NULL) { IOObjectRelease(e); return found; }
  }
  if (e != IO_OBJECT_NULL) IOObjectRelease(e);
  return IO_OBJECT_NULL;
}

static uint32_t bridge_levels(void) {
  uint32_t n = 0;
  for (io_service_t s; (s = find_bridge_dext(n + 1)) != IO_OBJECT_NULL; n++) IOObjectRelease(s);
  return n;
}

// see the protocol notes above. status 0 on a query or a reset that returned success, else status 1 and resp0 = the kern_return_t. resp1 = 1 always.
static void reset_link(const request_t *req, response_t *resp) {
  uint32_t kr = KERN_SUCCESS;
  resp->resp1 = 1;
  if (!req->arg1) { resp->resp0 = bridge_levels(); return; } // a query opens nothing
  if ((req->arg1 | req->arg2) >> 32) kr = KR_BAD_ARGUMENT;
  else {
    uint64_t in[2] = {req->arg1, req->arg2};
    if (req->arg2 & 1) __atomic_store_n(&g_terminating, 1, __ATOMIC_SEQ_CST);
    if (req->arg0 == 0) kr = dext_call(5, in, 2);
    else {
      io_service_t svc = req->arg0 <= MAX_REGISTRY_HOPS ? find_bridge_dext((uint32_t)req->arg0) : IO_OBJECT_NULL;
      io_connect_t conn;
      if (svc == IO_OBJECT_NULL) kr = KR_NO_DEVICE;
      else {
        kr = IOServiceOpen(svc, mach_task_self(), 0, &conn);
        IOObjectRelease(svc);
        if (kr == KERN_SUCCESS) {
          kr = IOConnectCallMethod(conn, 5, in, 2, NULL, 0, NULL, NULL, NULL, NULL);
          IOServiceClose(conn);
        }
      }
    }
  }
  if (kr != KERN_SUCCESS) { resp->status = 1; resp->resp0 = kr; }
}

// after the answer of a link reset went out: a terminate notification that arrived while it ran ends the server now (the client expects the connection to close)
static void end_link_reset(void) {
  __atomic_store_n(&g_terminating, 0, __ATOMIC_SEQ_CST);
  if (__atomic_load_n(&g_exit_pending, __ATOMIC_SEQ_CST)) _exit(0);
}

static int validate_bar(uint8_t bar, uint64_t off, uint64_t sz) {
  return (bar < MAX_BARS && g_bars[bar].addr && off + sz <= g_bars[bar].size && sz <= BULK_BUF_SIZE) ? 0 : -1;
}

static void cleanup(void) {
  for (int i = 0; i < MAX_BARS; i++)
    if (g_bars[i].addr) { IOConnectUnmapMemory64(g_conn, i, mach_task_self(), g_bars[i].addr); g_bars[i].addr = 0; }

  for (int i = 0; i < MAX_SYSMEM; i++) {
    if (!g_sysmem[i].live) continue;
    munmap((void*)g_sysmem[i].addr, g_sysmem[i].size);
    if (g_sysmem[i].shm_fd >= 0) { close(g_sysmem[i].shm_fd); shm_unlink(g_sysmem[i].shm_name); }
  }

  memset(g_sysmem, 0, sizeof(g_sysmem));
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
  while ((spin_for_request(fd), recv_request(fd, &req, &req_fd)) == 0) {
    resp = (response_t){0};
    if (req_fd >= 0 && req.cmd != CMD_MAP_FD) { close(req_fd); req_fd = -1; }

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

    case CMD_UNMAP_SYSMEM:
      unmap_sysmem(req.arg0, &resp);
      break;

    case CMD_MAP_FD: {
      uint64_t tab[MAX_SEGMENTS * 2 + 1];
      uint32_t nseg = 0;
      resp.status = map_fd(req_fd, req.arg0, req.arg1, &resp, tab, &nseg) ? 1 : 0;
      if (req_fd >= 0) close(req_fd); // the mapping keeps the object, the client keeps its own descriptor
      if (resp.status) { send_response(fd, &resp, -1); continue; }
      send_response(fd, &resp, -1);
      send(fd, tab, 8 + nseg * 16, 0); // nseg, then the (address, length) pairs
      continue;
    }

    case CMD_RESET_LINK:
      reset_link(&req, &resp);
      send_response(fd, &resp, -1);
      end_link_reset();
      continue;

    case CMD_CFG_READ: {
      uint64_t in[2] = {req.arg0, req.arg1};
      resp.status = dext_rpc(0, in, 2, &resp.resp0) ? 1 : 0;
      break;
    }

    case CMD_CFG_WRITE: {
      uint64_t in[3] = {req.arg0, req.arg1, req.arg2};
      resp.status = dext_rpc(1, in, 3, NULL) ? 1 : 0;
      break;
    }

    case CMD_RESIZE_BAR:
      break;

    case CMD_RESET:
      resp.status = dext_rpc(2, NULL, 0, NULL) ? 1 : 0;
      break;

    case CMD_MMIO_READ:
      if (validate_bar(req.bar, req.arg0, req.arg1)) { resp.status = 1; break; }
      mmio_copy(g_bulk_buf, (void*)(g_bars[req.bar].addr + req.arg0), req.arg1);
      resp.resp0 = req.arg1;
      send_response_data(fd, &resp, g_bulk_buf, req.arg1);
      continue;

    case CMD_FEATURES:
      resp.resp0 = g_reg_id;
      resp.resp1 = (uint64_t)PROTO_VERSION << 32 | FEAT_FEATURES | FEAT_READ_1SEND | FEAT_QOS_SPIN | (dext_has_wc() ? FEAT_WC_BAR0 : 0);
      break;

    case CMD_MMIO_WRITE:
      recvall(fd, g_bulk_buf, req.arg1);
      if (!validate_bar(req.bar, req.arg0, req.arg1))
        mmio_copy((void*)(g_bars[req.bar].addr + req.arg0), g_bulk_buf, req.arg1);
      continue;

    default:
      resp.status = 1;
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
  pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0); // the thread that serves the client: a request is latency, not throughput
  const char *spin = getenv("TINYGPU_SPIN_US");
  g_spin_ns = spin && *spin ? strtoull(spin, NULL, 0) * 1000ull : 0;
  if (g_spin_ns) printf("spinning %llu us after each answer\n", (unsigned long long)(g_spin_ns / 1000));

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
