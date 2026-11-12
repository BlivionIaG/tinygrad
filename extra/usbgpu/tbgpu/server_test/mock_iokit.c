// A stand-in for the IOKit calls server.c makes, so the REAL server.c runs in a test without a dext or a GPU. It models what
// TinyGPUDriverUserClient does with PrepareDMA / ReleaseDMA: an IOVA arena of MOCK_ARENA_MIB (default 2047, the Thunderbolt DART aperture),
// slots reused lowest first, (RELEASE_MAGIC, slot) after the address table, every mapping freed when the connection closes.
// Environment: MOCK_ARENA_MIB, MOCK_SEG_MIB (split every mapping into segments of that size), MOCK_OLD_DEXT=1 (a dext without ReleaseDMA:
// no magic, selector 4 unsupported), MOCK_FAIL_RELEASE=N (the Nth ReleaseDMA fails with kIOReturnBusy and leaves the mapping).
// Link reset: the service plane above the `tinygpu` service is modelled as on the real Mac (display nub, IOPP, nub of the card's switch downstream port,
// IOPP, switch upstream port, IOPP, ASM2464PD downstream port, IOPP, ASM2464PD upstream port, IOPP, Apple root port nub, ...); the first MOCK_BRIDGES (default 4,
// at most 4, none with MOCK_OLD_DEXT=1) nubs from the GPU up have a `tinygpubridge` child. Selector 5 of the GPU connection (level 0) and of the connection of the
// bridge dext at level k appends the line `<level> <type> <options>` to the file MOCK_LOG and returns MOCK_RESET_KR_LEVELk, else MOCK_RESET_KR (default 0);
// MOCK_OPEN_KR_LEVELk makes IOServiceOpen of that bridge dext fail, MOCK_TERMINATE_NOTIFY=1 delivers kIOMessageServiceIsTerminated from inside a reset that has
// options & 1. CfgRead (selector 0) at offset 0xF000 / 0xF004 / 0xF008 answers the number of open bridge connections / of unreleased registry objects / of
// releases and closes that did not match an open one: a test asks after each request that the server holds nothing. 0xF00C: sendmsg + send calls the server made so far,
// 0xF010: the QoS class of the serving thread. MOCK_REG_ID: the registry entry id of the service (default 0x100000a5c). Selector 6 (Features, WC BARs) is
// unsupported with MOCK_OLD_DEXT=1 or MOCK_NO_WC=1. A BAR type 0xF0000100 | bar maps the same memory as the plain one.
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <mach/mach.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/IOMessage.h>
#include <sys/socket.h>
#include <pthread/qos.h>

#define KR_NO_MEMORY 0xE00002BDu
#define KR_BAD_ARG 0xE00002C2u
#define KR_UNSUPPORTED 0xE00002C7u
#define KR_BUSY 0xE00002D5u
#define MAGIC 0x52454C444D410001ull
#define ARENA_BASE 0x80000000ull
#define PAGE (16ull << 10)
#define MAX_SLOTS 4096

static struct { int live; uint64_t iova, size; } slots[MAX_SLOTS];
static int nslots, release_calls;
static void *bar_mem[6];
const mach_port_t kIOMainPortDefault = 0;

static uint64_t env_u64(const char *n, uint64_t d) { const char *v = getenv(n); return v && *v ? strtoull(v, NULL, 0) : d; }

static int arena_alloc(uint64_t size, uint64_t *iova) { // lowest free run of `size` bytes
  uint64_t cap = env_u64("MOCK_ARENA_MIB", 2047) << 20, at = 0;
  for (int moved = 1; moved;) {
    moved = 0;
    for (int i = 0; i < nslots; i++) {
      uint64_t lo = slots[i].iova - ARENA_BASE, hi = lo + slots[i].size;
      if (slots[i].live && lo < at + size && hi > at) { at = hi; moved = 1; }
    }
  }
  if (at + size > cap) return -1;
  *iova = ARENA_BASE + at;
  return 0;
}

static void free_all(void) { memset(slots, 0, sizeof slots); nslots = 0; release_calls = 0; }

// registry model
enum { N_ENT = 14, FIRST_DEXT = 20, N_HANDLE = 1 << 16, BRIDGE_CONN = 100, GPU_CONN = 1 };
static const struct { const char *name; int pci, parent; } ents[N_ENT] = {
  {"tinygpu", 0, 1}, {"display", 1, 2}, {"IOPP", 0, 3}, {"pci-bridge", 1, 4}, {"IOPP", 0, 5}, {"pci-bridge", 1, 6}, {"IOPP", 0, 7},
  {"pci-bridge", 1, 8}, {"IOPP", 0, 9}, {"pci-bridge", 1, 10}, {"IOPP", 0, 11}, {"pcic2-bridge", 1, 12}, {"pcic2", 0, 13}, {"IOPlatformExpertDevice", 0, -1},
};
static const int bridge_nub[5] = {0, 3, 5, 7, 9}; // the nub of level k
static struct { int ent, live, iterator, pos; } handles[N_HANDLE]; // io_object_t = index + 1000
static int nhandles, live_handles, bad_closes, open_bridge[5];
static IOServiceInterestCallback term_cb;

static int n_bridges(void) { return env_u64("MOCK_OLD_DEXT", 0) ? 0 : (int)env_u64("MOCK_BRIDGES", 4) > 4 ? 4 : (int)env_u64("MOCK_BRIDGES", 4); }
static io_object_t new_handle(int ent, int iterator) {
  if (nhandles == N_HANDLE) abort();
  handles[nhandles] = (typeof(handles[0])){.ent = ent, .live = 1, .iterator = iterator};
  live_handles++;
  return 1000 + nhandles++;
}
static int handle_ent(io_object_t o) { return o >= 1000 && o - 1000 < (io_object_t)nhandles && handles[o - 1000].live ? handles[o - 1000].ent : -1; }
static int ent_parent(int ent) { // the entity above ent (a dext has its nub), -1 at the top
  if (ent >= FIRST_DEXT) return bridge_nub[ent - FIRST_DEXT];
  return ents[ent].parent < 0 || ents[ent].parent >= N_ENT ? -1 : ents[ent].parent;
}
static int ent_exists(int ent) { return ent < FIRST_DEXT ? ent >= 0 && ent < N_ENT : ent - FIRST_DEXT >= 1 && ent - FIRST_DEXT <= n_bridges(); }

CFMutableDictionaryRef IOServiceNameMatching(const char *name) { (void)name; return NULL; }
io_service_t IOServiceGetMatchingService(mach_port_t p, CFDictionaryRef m) { (void)p; (void)m; return new_handle(0, 0); }
IONotificationPortRef IONotificationPortCreate(mach_port_t p) { (void)p; return (IONotificationPortRef)1; }
void IONotificationPortSetDispatchQueue(IONotificationPortRef n, dispatch_queue_t q) { (void)n; (void)q; }
kern_return_t IOServiceAddInterestNotification(IONotificationPortRef n, io_service_t s, const io_name_t t, IOServiceInterestCallback c, void *r, io_object_t *o) {
  (void)n; (void)s; (void)t; (void)r; term_cb = c; *o = 1; return 0; // handle 1 stands for the notification, which the server keeps for good
}
kern_return_t IOObjectRelease(io_object_t o) {
  if (o == 1) return 0;
  if (handle_ent(o) < 0) { bad_closes++; return KR_BAD_ARG; }
  handles[o - 1000].live = 0; live_handles--;
  return 0;
}
boolean_t IOObjectConformsTo(io_object_t o, const io_name_t name) { int e = handle_ent(o); return e >= 0 && e < N_ENT && ents[e].pci && !strcmp(name, "IOPCIDevice"); }
kern_return_t IORegistryEntryGetRegistryEntryID(io_registry_entry_t o, uint64_t *id) { (void)o; *id = env_u64("MOCK_REG_ID", 0x100000a5cull); return 0; }

unsigned long mock_sends; // sendmsg + send calls of server.c (mock_sys.h)
ssize_t mock_sendmsg(int fd, const struct msghdr *msg, int flags) { mock_sends++; return (sendmsg)(fd, msg, flags); }
ssize_t mock_send(int fd, const void *buf, size_t len, int flags) { mock_sends++; return (send)(fd, buf, len, flags); }

kern_return_t IORegistryEntryGetName(io_registry_entry_t o, io_name_t name) {
  int e = handle_ent(o);
  if (e < 0 || handles[o - 1000].iterator) return KR_BAD_ARG;
  strcpy(name, e >= FIRST_DEXT ? "tinygpubridge" : ents[e].name);
  return 0;
}
kern_return_t IORegistryEntryGetParentEntry(io_registry_entry_t o, const io_name_t plane, io_registry_entry_t *parent) {
  int e = handle_ent(o);
  if (e < 0 || handles[o - 1000].iterator || strcmp(plane, kIOServicePlane)) return KR_BAD_ARG;
  int p = ent_parent(e);
  if (p < 0) return 0xE00002F0u; // kIOReturnNotFound
  *parent = new_handle(p, 0);
  return 0;
}
kern_return_t IORegistryEntryGetChildIterator(io_registry_entry_t o, const io_name_t plane, io_iterator_t *it) {
  int e = handle_ent(o);
  if (e < 0 || handles[o - 1000].iterator || strcmp(plane, kIOServicePlane)) return KR_BAD_ARG;
  *it = new_handle(e, 1);
  return 0;
}
static int candidate(int i) { return i < N_ENT ? i : i < N_ENT + 4 ? FIRST_DEXT + 1 + i - N_ENT : -1; }
io_object_t IOIteratorNext(io_iterator_t it) { // the entities whose parent is the iterated one, in index order (dexts last)
  int e = handle_ent(it);
  if (e < 0 || !handles[it - 1000].iterator) return 0;
  for (int c; (c = candidate(handles[it - 1000].pos++)) >= 0; ) if (ent_exists(c) && ent_parent(c) == e) return new_handle(c, 0);
  return 0;
}
kern_return_t IOServiceOpen(io_service_t s, task_port_t t, uint32_t type, io_connect_t *c) {
  (void)t; (void)type;
  int e = s < 1000 ? 0 : handle_ent(s);
  if (e < 0) return KR_BAD_ARG;
  if (e < FIRST_DEXT) { free_all(); *c = GPU_CONN; return 0; }
  char name[32];
  snprintf(name, sizeof name, "MOCK_OPEN_KR_LEVEL%d", e - FIRST_DEXT);
  if (env_u64(name, 0)) return (kern_return_t)env_u64(name, 0);
  open_bridge[e - FIRST_DEXT]++;
  *c = BRIDGE_CONN + e - FIRST_DEXT;
  return 0;
}
kern_return_t IOServiceClose(io_connect_t c) { // Stop_Impl: CompleteDMA on every command
  if (c >= BRIDGE_CONN && c < BRIDGE_CONN + 5) { if (open_bridge[c - BRIDGE_CONN]-- <= 0) { open_bridge[c - BRIDGE_CONN] = 0; bad_closes++; } return 0; }
  free_all();
  return 0;
}
kern_return_t IOConnectMapMemory64(io_connect_t c, uint32_t type, task_port_t t, mach_vm_address_t *a, mach_vm_size_t *s, IOOptionBits o) {
  (void)c; (void)t; (void)o;
  if (type >= 6 && (type & ~0xFFu) == 0xF0000100u) type &= 0xFF; // a write-combined BAR: the same memory in the mock
  if (type >= 6) return KR_BAD_ARG;
  if (!bar_mem[type]) bar_mem[type] = calloc(1, 1 << 20);
  *a = (mach_vm_address_t)bar_mem[type]; *s = 1 << 20;
  return 0;
}
kern_return_t IOConnectUnmapMemory64(io_connect_t c, uint32_t type, task_port_t t, mach_vm_address_t a) { (void)c; (void)type; (void)t; (void)a; return 0; }

// selector 5 (ResetLink) of the GPU's user client (level 0) or of a bridge dext's (level k)
static kern_return_t reset_link_call(int level, uint32_t in_cnt, const uint64_t *in) {
  if (in_cnt != 2) return KR_BAD_ARG;
  char name[32];
  snprintf(name, sizeof name, "MOCK_RESET_KR_LEVEL%d", level);
  kern_return_t kr = (kern_return_t)env_u64(name, env_u64("MOCK_RESET_KR", 0));
  const char *log = getenv("MOCK_LOG");
  if (log && *log) { FILE *f = fopen(log, "a"); if (f) { fprintf(f, "%d %llu %llu\n", level, in[0], in[1]); fclose(f); } }
  if (kr == 0 && (in[1] & 1) && env_u64("MOCK_TERMINATE_NOTIFY", 0) && term_cb) term_cb(NULL, 1, kIOMessageServiceIsTerminated, NULL);
  return kr;
}

kern_return_t IOConnectCallMethod(mach_port_t c, uint32_t sel, const uint64_t *in, uint32_t in_cnt, const void *is, size_t isc, uint64_t *out, uint32_t *out_cnt, void *os, size_t *osc) {
  (void)is; (void)isc; (void)os; (void)osc;
  if (c >= BRIDGE_CONN && c < BRIDGE_CONN + 5) return sel == 5 ? reset_link_call(c - BRIDGE_CONN, in_cnt, in) : KR_UNSUPPORTED; // a bridge dext knows only ResetLink
  switch (sel) {
  case 0: { // CfgRead: vendor id, or the debug window
    if (in_cnt != 2 || *out_cnt < 1) return KR_BAD_ARG;
    int open = 0;
    for (int i = 1; i < 5; i++) open += open_bridge[i];
    out[0] = in[0] == 0 ? 0x1002 : in[0] == 0xF000 ? (uint64_t)open : in[0] == 0xF004 ? (uint64_t)live_handles : in[0] == 0xF008 ? (uint64_t)bad_closes
           : in[0] == 0xF00C ? mock_sends : in[0] == 0xF010 ? (uint64_t)qos_class_self() : 0;
    *out_cnt = 1;
    return 0;
  }
  case 6: // Features: bit 0 = write-combined BARs
    if (*out_cnt < 1) return KR_BAD_ARG;
    if (env_u64("MOCK_OLD_DEXT", 0) || env_u64("MOCK_NO_WC", 0)) return KR_UNSUPPORTED;
    out[0] = 1; *out_cnt = 1;
    return 0;
  case 1: case 2: return 0; // CfgWrite, Reset
  case 5: return env_u64("MOCK_OLD_DEXT", 0) ? KR_UNSUPPORTED : reset_link_call(0, in_cnt, in); // a dext without ResetLink
  case 4: { // ReleaseDMA
    if (env_u64("MOCK_OLD_DEXT", 0)) return KR_UNSUPPORTED;
    if (in_cnt != 1 || in[0] >= (uint64_t)nslots || !slots[in[0]].live) return KR_BAD_ARG;
    if (++release_calls == (int)env_u64("MOCK_FAIL_RELEASE", 0)) return KR_BUSY;
    slots[in[0]].live = 0;
    return 0;
  }
  }
  return KR_UNSUPPORTED;
}

kern_return_t IOConnectCallStructMethod(mach_port_t c, uint32_t sel, const void *in, size_t in_sz, void *out, size_t *out_sz) {
  (void)c; (void)in;
  if (sel != 3) return KR_UNSUPPORTED;
  if (in_sz < 4097 || *out_sz < 4097 || in_sz % PAGE) return KR_BAD_ARG; // "buffers >= 4097 bytes" (and whole 16 KiB pages)
  int slot = -1;
  for (int i = 0; i < nslots; i++) if (!slots[i].live) { slot = i; break; }
  if (slot < 0) { if (nslots == MAX_SLOTS) return KR_NO_MEMORY; slot = nslots; }
  uint64_t iova;
  if (arena_alloc(in_sz, &iova)) return KR_NO_MEMORY;
  uint64_t *t = out, seg = env_u64("MOCK_SEG_MIB", 0) << 20, n = 0;
  memset(out, 0, *out_sz);
  for (uint64_t o = 0; o < in_sz; o += seg ? seg : in_sz, n++) {
    if (n == 32) return KR_NO_MEMORY; // more segments than the dext's array
    t[2 * n] = iova + o; t[2 * n + 1] = (seg && in_sz - o > seg) ? seg : in_sz - o;
  }
  if (!env_u64("MOCK_OLD_DEXT", 0)) { t[2 * n + 2] = MAGIC; t[2 * n + 3] = slot; }
  slots[slot].live = 1; slots[slot].iova = iova; slots[slot].size = in_sz;
  if (slot == nslots) nslots++;
  return 0;
}
