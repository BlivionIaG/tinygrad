#!/usr/bin/env python3
# Runs the REAL extra/usbgpu/tbgpu/installer/Shared/server.c (built against mock_iokit.c, no dext, no GPU, macOS only) and speaks the wire
# protocol to it: MAP_SYSMEM_FD / UNMAP_SYSMEM / MAP_FD, the 128 live block limit, a dext without ReleaseDMA, a refused release, the DMA arena
# cap, cleanup on disconnect, and extra/hcq1/remote.py's APLRemotePCIDevice against it.
#   python3 -m pytest extra/usbgpu/tbgpu/server_test/test_dma.py -n12
# TINYGPU_TEST_SERVER_C=<server.c> builds another server source (the one before this feature, for example) instead: the tests that need
# UNMAP_SYSMEM / MAP_FD then fail. It is for testing the tests only.
import array, mmap, os, socket, struct, subprocess, sys, tempfile, threading, time, unittest
try: import _posixshmem
except ImportError: _posixshmem = None  # type: ignore[assignment] # the tests skip themselves off macOS, the module still imports

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
SERVER_C = os.environ.get("TINYGPU_TEST_SERVER_C") or os.path.join(HERE, "..", "installer", "Shared", "server.c")
REQ, RESP = '<BIIQQQ', '<BQQ'
MAP_BAR, MAP_SYSMEM_FD, CFG_READ, MMIO_READ, MMIO_WRITE, UNMAP_SYSMEM, MAP_FD = 1, 2, 3, 6, 7, 15, 16
KR_BAD_ARG, KR_UNSUPPORTED, KR_BUSY = 0xE00002C2, 0xE00002C7, 0xE00002D5
MAGIC, PAGE, MIB = 0x52454C444D410001, 16384, 1 << 20

# Every server process names its shm objects "/tinygpu_<n>" and the tests run in parallel: give each one its own namespace (and let a test see them).
SHM_PREFIX_H = """
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
static inline const char *tg_name(const char *n) { static char b[32]; snprintf(b, sizeof b, "/t%d_%s", (int)getpid(), n + 1); return b; }
#define shm_open(n, ...) shm_open(tg_name(n), __VA_ARGS__)
#define shm_unlink(n) shm_unlink(tg_name(n))
"""

def build_server(tmp:str) -> str:
  shm_h, binary = os.path.join(tmp, "shm_prefix.h"), os.path.join(tmp, "server")
  with open(shm_h, "w") as f: f.write(SHM_PREFIX_H)
  srcs = [SERVER_C, os.path.join(HERE, "mock_iokit.c"), os.path.join(HERE, "mock_main.c")]
  subprocess.run(["xcrun", "clang", "-O1", "-include", shm_h, "-o", binary, *srcs], check=True)
  return binary

def shm_exists(name:str) -> bool:
  try: os.close(_posixshmem.shm_open(name, os.O_RDONLY, 0o600))
  except FileNotFoundError: return False
  return True

def shm_object(size:int) -> int: # a POSIX shm object, unlinked at once: the memory of a client
  name = f"/tgs{os.getpid() % 10**6}x{time.monotonic_ns() % 10**9}"
  fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_RDWR, 0o600)
  _posixshmem.shm_unlink(name)
  os.ftruncate(fd, size)
  return fd

class Client:
  def __init__(self, path:str):
    self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    for _ in range(200):
      try:
        self.s.connect(path)
        break
      except (FileNotFoundError, ConnectionRefusedError): time.sleep(0.02)
    else: raise RuntimeError("no server")
    self.s.settimeout(20)
    assert self.rpc(CFG_READ, 0, 2)[:2] == (0, 0x1002)  # the handshake

  def recvall(self, n:int) -> bytes:
    out = b""
    while len(out) < n:
      d = self.s.recv(n - len(out))
      if not d: raise ConnectionError("closed")
      out += d
    return out

  def rpc(self, cmd:int, a0=0, a1=0, a2=0, fd:int|None=None, bar=0) -> tuple[int, int, int]:
    pkt = struct.pack(REQ, cmd, 0, bar, a0, a1, a2)
    if fd is None: self.s.sendall(pkt)
    else: self.s.sendmsg([pkt], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('i', fd))])
    return struct.unpack(RESP, self.recvall(struct.calcsize(RESP)))

  def map_sysmem(self, size:int) -> tuple[int, int, mmap.mmap, list[tuple[int, int]]]:
    pkt = struct.pack(REQ, MAP_SYSMEM_FD, 0, 0, size, 0, 0)
    self.s.sendall(pkt)
    msg, anc, _, _ = self.s.recvmsg(struct.calcsize(RESP), socket.CMSG_LEN(4))
    status, mapped, idx = struct.unpack(RESP, msg + self.recvall(struct.calcsize(RESP) - len(msg)))
    if status or not anc: return status, 0, None, []  # type: ignore[return-value]
    fd = struct.unpack('i', anc[0][2][:4])[0]
    mm = mmap.mmap(fd, mapped)
    os.close(fd)
    tab = array.array('Q', mm[:8192])
    pairs = []
    for i in range(0, len(tab), 2):
      if tab[i] == 0 and tab[i + 1] == 0: break
      pairs.append((tab[i], tab[i + 1]))
    return status, idx, mm, pairs

  def map_fd_reply(self) -> tuple[int, int, int, list[tuple[int, int]]]:
    status, r0, r1 = struct.unpack(RESP, self.recvall(struct.calcsize(RESP)))
    if status: return status, r0, r1, []
    nseg = struct.unpack('<Q', self.recvall(8))[0]
    raw = struct.unpack(f'<{2 * nseg}Q', self.recvall(16 * nseg))
    return status, r0, r1, list(zip(raw[0::2], raw[1::2]))

  def map_fd(self, fd:int, off:int, size:int) -> tuple[int, int, int, list[tuple[int, int]]]:
    self.s.sendmsg([struct.pack(REQ, MAP_FD, 0, 0, off, size, 0)], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('i', fd))])
    return self.map_fd_reply()

  def close(self): self.s.close()

class ServerCase(unittest.TestCase):
  tmpdir: tempfile.TemporaryDirectory[str]
  tmp: str
  binary: str

  @classmethod
  def setUpClass(cls):
    if sys.platform != "darwin": raise unittest.SkipTest("server.c is macOS only")
    cls.tmpdir = tempfile.TemporaryDirectory(prefix="tgdma")
    cls.tmp = cls.tmpdir.name
    cls.binary = build_server(cls.tmp)

  @classmethod
  def tearDownClass(cls): cls.tmpdir.cleanup()

  def start(self, nofile:int|None=None, **env:str) -> Client:
    self.path = os.path.join(self.tmp, f"s{time.monotonic_ns()}.sock")
    cmd = [self.binary, self.path] if nofile is None else ["/bin/sh", "-c", f'ulimit -n {nofile}; exec "$0" "$1"', self.binary, self.path]
    self.proc = subprocess.Popen(cmd, env=dict(os.environ, **env), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    self.addCleanup(lambda: (self.proc.kill(), self.proc.wait()))
    c = Client(self.path)
    self.addCleanup(c.close)
    return c

  def shm(self, idx:int) -> str: return f"/t{self.proc.pid}_tinygpu_{idx}" # the server's shm object of block idx

  def store(self, size:int) -> tuple[int, mmap.mmap]: # a POSIX shm object the test owns, like the memory of a client
    fd = shm_object(size)
    self.addCleanup(os.close, fd)
    return fd, mmap.mmap(fd, size)

class TestRelease(ServerCase):
  def test_map_table_and_unmap(self):
    c = self.start(MOCK_SLOT_BASE="17")
    st, idx, mm, pairs = c.map_sysmem(2 * MIB)
    self.assertEqual((st, idx), (0, 0))
    self.assertEqual(sum(n for _, n in pairs), 2 * MIB)
    self.assertEqual(struct.unpack_from('<QQ', mm, 16 * len(pairs) + 16), (MAGIC, 17))  # dext slot differs from server index
    mm[:8192] = bytes(8192)  # the table sits at the head of the block, the client zeroes it
    mm[16384:16388] = b"data"
    self.assertTrue(shm_exists(self.shm(0)))
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, 2 * MIB, 1))
    self.assertFalse(shm_exists(self.shm(0)))  # the server's own object is gone, the client's mapping of it is not
    self.assertEqual(bytes(mm[16384:16388]), b"data")
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_BAD_ARG, 1))  # a dead index
    self.assertEqual(c.rpc(UNMAP_SYSMEM, 5000), (1, KR_BAD_ARG, 1))
    st, idx2, _, _ = c.map_sysmem(MIB)
    self.assertEqual((st, idx2), (0, 0))  # the index is reused

  def test_unknown_command_is_the_old_server_answer(self):
    self.assertEqual(self.start().rpc(99), (1, 0, 0))

  def test_live_block_limit_and_churn(self):
    c = self.start()
    live = [c.map_sysmem(PAGE)[:2] for _ in range(128)]
    self.assertTrue(all(st == 0 for st, _ in live))
    self.assertEqual(sorted(i for _, i in live), list(range(128)))
    self.assertEqual(c.map_sysmem(PAGE)[0], 1)  # the 129th live block
    self.assertEqual(c.rpc(UNMAP_SYSMEM, 77)[0], 0)
    self.assertEqual(c.map_sysmem(PAGE)[:2], (0, 77))
    self.assertEqual(c.rpc(UNMAP_SYSMEM, 77)[0], 0)
    for _ in range(600):  # far more than 128 blocks in total, 127 of them live all the time
      st, idx, mm, _ = c.map_sysmem(PAGE)
      self.assertEqual((st, idx), (0, 77))
      self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[:2], (0, PAGE))

  def test_churn_mixed_with_fd_blocks(self):
    c = self.start()
    fd, _ = self.store(PAGE)
    live = [c.map_sysmem(PAGE)[1] for _ in range(64)] + [c.map_fd(fd, 0, PAGE)[2] for _ in range(64)]
    self.assertEqual(sorted(live), list(range(128)))
    self.assertEqual(c.map_fd(fd, 0, PAGE)[0], 1)  # the limit counts both kinds
    for _ in range(300):
      for idx in (3, 100):
        self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)
      self.assertEqual(c.map_sysmem(PAGE)[1], 3)
      self.assertEqual(c.map_fd(fd, 0, PAGE)[2], 100)

  def test_arena_cap_and_space_returns(self):
    c = self.start(MOCK_ARENA_MIB="64")
    a, b = c.map_sysmem(32 * MIB), c.map_sysmem(32 * MIB)
    self.assertEqual((a[0], b[0]), (0, 0))
    self.assertEqual(c.map_sysmem(MIB)[0], 1)  # the arena is full
    self.assertEqual(c.rpc(UNMAP_SYSMEM, a[1])[0], 0)
    st, _, _, pairs = c.map_sysmem(32 * MIB)
    self.assertEqual(st, 0)
    self.assertEqual(pairs[0][0], a[3][0][0])  # the freed hole is what is handed out again
    self.assertEqual(c.map_sysmem(MIB)[0], 1)

  def test_segments(self):
    c = self.start(MOCK_SEG_MIB="1")
    st, idx, mm, pairs = c.map_sysmem(4 * MIB)
    self.assertEqual((st, len(pairs)), (0, 4))
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)

  def test_old_dext(self):
    c = self.start(MOCK_OLD_DEXT="1")
    st, idx, mm, pairs = c.map_sysmem(MIB)
    self.assertEqual(st, 0)
    self.assertEqual(struct.unpack_from('<QQ', mm, 16 * len(pairs) + 16), (0, 0))  # no mark: an old client sees what it always saw
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_UNSUPPORTED, 1))  # understood, the dext cannot
    self.assertEqual(c.map_sysmem(MIB)[:2], (0, 1))  # and the block stays live
    self.assertTrue(shm_exists(self.shm(0)))

  def test_old_dext_map_fd(self):
    c = self.start(MOCK_OLD_DEXT="1")
    fd, _ = self.store(MIB)
    st, size, idx, pairs = c.map_fd(fd, 0, MIB)
    self.assertEqual((st, size, idx), (0, MIB, 0))  # importing works, giving it back does not
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_UNSUPPORTED, 1))
    self.assertEqual(c.map_fd(fd, 0, MIB)[2], 1)

  def test_refused_release_keeps_the_block(self):
    c = self.start(MOCK_FAIL_RELEASE="1")
    st, idx, _, _ = c.map_sysmem(MIB)
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_BUSY, 1))
    self.assertTrue(shm_exists(self.shm(idx)))  # nothing was given back
    self.assertEqual(c.map_sysmem(MIB)[:2], (0, 1))  # still live: the next block gets another index
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, MIB, 1))  # the retry works
    self.assertFalse(shm_exists(self.shm(idx)))

  def test_refused_release_keeps_the_aperture(self):
    c = self.start(MOCK_ARENA_MIB="64", MOCK_FAIL_RELEASE="1")
    st, idx, _, _ = c.map_sysmem(64 * MIB)
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_BUSY, 1))
    self.assertEqual(c.map_sysmem(MIB)[0], 1)  # the DMA mapping is still there
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)
    self.assertEqual(c.map_sysmem(64 * MIB)[:2], (0, 0))

  def test_disconnect_frees_everything(self):
    c = self.start(MOCK_ARENA_MIB="64")
    fd, _ = self.store(32 * MIB)
    self.assertEqual(c.map_sysmem(32 * MIB)[:2], (0, 0))
    self.assertEqual(c.map_fd(fd, 0, 32 * MIB)[:3], (0, 32 * MIB, 1))
    self.assertTrue(shm_exists(self.shm(0)))
    c.close()
    c2 = Client(self.path)  # served after the first connection is cleaned up
    self.assertFalse(shm_exists(self.shm(0)))
    self.assertEqual(c2.map_sysmem(32 * MIB)[:2], (0, 0))
    self.assertEqual(c2.map_fd(fd, 0, 32 * MIB)[:3], (0, 32 * MIB, 1))
    live = [c2.rpc(UNMAP_SYSMEM, i)[0] for i in (0, 1)]
    self.assertEqual(live, [0, 0])

  def test_disconnect_with_all_blocks_live(self):
    c = self.start()
    for _ in range(128): self.assertEqual(c.map_sysmem(PAGE)[0], 0)
    c.close()
    c2 = Client(self.path)
    self.assertFalse(any(shm_exists(self.shm(i)) for i in range(128)))
    self.assertEqual([c2.map_sysmem(PAGE)[1] for _ in range(128)], list(range(128)))

  def test_mmio_still_works(self):
    c = self.start()
    self.assertEqual(c.rpc(MAP_BAR, bar=0)[0], 0)
    data = b"payload crossing the request boundary"
    c.s.sendall(struct.pack(REQ, MMIO_WRITE, 0, 0, 16, len(data), 0) + data)
    self.assertEqual(c.rpc(MMIO_READ, 16, len(data))[:2], (0, len(data)))
    self.assertEqual(c.recvall(len(data)), data)
    self.assertEqual(c.rpc(CFG_READ, 0, 2), (0, 0x1002, 0))

class TestMapFd(ServerCase):
  def test_map_unmap_and_data_untouched(self):
    c = self.start()
    fd, mm = self.store(8 * MIB)
    pat = bytes(range(256)) * (8 * MIB // 256)
    mm[:] = pat
    st, size, idx, pairs = c.map_fd(fd, 2 * PAGE, 4 * MIB)
    self.assertEqual((st, size, idx), (0, 4 * MIB, 0))
    self.assertEqual(sum(n for _, n in pairs), 4 * MIB)
    self.assertEqual(bytes(mm[:]), pat)  # the server wrote nothing into the memory
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, 4 * MIB, 1))
    self.assertEqual(bytes(mm[:]), pat)
    self.assertEqual(c.map_fd(fd, 0, 4 * MIB)[2], 0)  # the index is back

  def test_segment_table(self):
    c = self.start(MOCK_SEG_MIB="1")
    fd, _ = self.store(8 * MIB)
    st, size, idx, pairs = c.map_fd(fd, 0, 4 * MIB)
    self.assertEqual((st, size, len(pairs)), (0, 4 * MIB, 4))
    self.assertEqual(pairs, [(0x80000000 + i * MIB, MIB) for i in range(4)])
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))  # without a payload left on the wire

  def test_dext_refuses_the_mapping(self):
    c = self.start(MOCK_SEG_MIB="1")
    fd, _ = self.store(40 * MIB)
    self.assertEqual(c.map_fd(fd, 0, 33 * MIB)[0], 1)
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))
    self.assertEqual(c.map_fd(fd, 0, 4 * MIB)[2], 0)  # the failed request did not take an index

  def test_bad_arguments(self):
    c = self.start()
    fd, _ = self.store(4 * MIB)
    self.assertEqual(c.map_fd(fd, 100, 2 * PAGE)[0], 1)  # offset not a multiple of 16 KiB
    self.assertEqual(c.map_fd(fd, PAGE // 2, 2 * PAGE)[0], 1)  # nor a multiple of the 4 KiB page
    self.assertEqual(c.map_fd(fd, 0, 100)[0], 1)  # size neither
    self.assertEqual(c.map_fd(fd, 0, 0)[0], 1)
    self.assertEqual(c.rpc(MAP_FD, 0, PAGE)[0], 1)  # no fd with the request
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))  # the connection is fine
    self.assertEqual(c.map_fd(fd, 0, PAGE)[2], 0)  # and none of that took an index

  def test_range_must_lie_inside_the_object(self):
    c = self.start()
    fd, mm = self.store(4 * MIB)
    mm[:] = bytes(range(256)) * (4 * MIB // 256)
    self.assertEqual(c.map_fd(fd, 0, 4 * MIB + PAGE)[0], 1)  # past the end
    self.assertEqual(c.map_fd(fd, 2 * MIB, 4 * MIB)[0], 1)
    self.assertEqual(c.map_fd(fd, 4 * MIB, PAGE)[0], 1)  # starts at the end
    self.assertEqual(c.map_fd(fd, 4 * MIB + PAGE, PAGE)[0], 1)  # beyond it
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))
    self.assertEqual(c.map_fd(fd, 2 * MIB, 2 * MIB)[:3], (0, 2 * MIB, 0))  # exactly up to the end
    self.assertEqual(c.map_fd(fd, 4 * MIB - PAGE, PAGE)[:3], (0, PAGE, 1))
    self.assertEqual(bytes(mm[:]), bytes(range(256)) * (4 * MIB // 256))

  def test_no_overflow_in_the_range(self):
    c = self.start()
    fd, _ = self.store(4 * MIB)
    for off, size in [(2**64 - PAGE, 2 * PAGE), (2**64 - 2 * PAGE, 2 * PAGE), (2**63 - PAGE, PAGE), (2**63, PAGE), (PAGE, 2**64 - PAGE),
                      (2**63 - PAGE, 2**63), (0, 2**63), (0, 2**64 - PAGE), (PAGE, 2**63 - PAGE)]:
      self.assertEqual(c.map_fd(fd, off, size)[:3], (1, 0, 0), (off, size))
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))
    self.assertEqual(c.map_fd(fd, 0, PAGE)[2], 0)

  def test_not_a_memory_object(self):
    c = self.start()
    for fd in (os.open("/dev/null", os.O_RDWR), os.open("/dev/zero", os.O_RDWR), os.open(self.tmp, os.O_RDONLY)):
      self.addCleanup(os.close, fd)
      self.assertEqual(c.map_fd(fd, 0, PAGE)[:3], (1, 0, 0))
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))

  def test_the_server_keeps_the_object(self):
    c = self.start()
    fd, mm = self.store(2 * MIB)
    dup = os.dup(fd)
    st, size, idx, _ = c.map_fd(dup, 0, 2 * MIB)
    os.close(dup)  # the client lets go of its descriptor, the block is still there
    self.assertEqual((st, size), (0, 2 * MIB))
    mm[:4] = b"live"
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, 2 * MIB, 1))
    self.assertEqual(bytes(mm[:4]), b"live")

  def test_request_split_across_sends(self):
    c = self.start()
    fd, _ = self.store(PAGE)
    pkt = struct.pack(REQ, MAP_FD, 0, 0, 0, PAGE, 0)
    c.s.sendmsg([pkt[:1]], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('i', fd))])  # the fd comes with the first byte only
    for part in (pkt[1:5], pkt[5:20], pkt[20:]):
      time.sleep(0.1)
      c.s.sendall(part)
    st, size, idx, pairs = c.map_fd_reply()
    self.assertEqual((st, size, idx, sum(n for _, n in pairs)), (0, PAGE, 0, PAGE))
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)

  def test_descriptors_are_not_leaked(self):
    c = self.start(nofile=64)  # a server that kept every descriptor it was sent would run dry long before 400 requests
    fd, _ = self.store(PAGE)
    for _ in range(100):
      self.assertEqual(c.rpc(CFG_READ, 0, 2, fd=fd)[:2], (0, 0x1002))  # an fd with a command that has no use for it
      self.assertEqual(c.rpc(UNMAP_SYSMEM, 5000, fd=fd), (1, KR_BAD_ARG, 1))
      self.assertEqual(c.map_fd(fd, 1, PAGE)[0], 1)  # a refused import
      st, _, idx, _ = c.map_fd(fd, 0, PAGE)  # and one that is served, then given back
      self.assertEqual(st, 0)
      self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)

  def test_arena_shared_with_sysmem_fd(self):
    c = self.start(MOCK_ARENA_MIB="64")
    fd, _ = self.store(64 * MIB)
    st, _, idx, _ = c.map_fd(fd, 0, 48 * MIB)
    self.assertEqual(st, 0)
    self.assertEqual(c.map_sysmem(32 * MIB)[0], 1)
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[0], 0)
    self.assertEqual(c.map_sysmem(32 * MIB)[0], 0)

  def test_map_unmap_cycles(self):
    c = self.start(MOCK_ARENA_MIB="64")
    fd, _ = self.store(64 * MIB)
    for i in range(500):
      st, _, idx, pairs = c.map_fd(fd, 0, 64 * MIB)
      self.assertEqual(st, 0)
      self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[:2], (0, 64 * MIB))

PRELUDE = """
import _posixshmem, os
from extra.hcq1.remote import APLRemotePCIDevice
APLRemotePCIDevice.ensure_app = classmethod(lambda cls: None) # the TinyGPU.app is not needed, the server under test is already running
d = APLRemotePCIDevice("AM", "usb4")
def shm_object(size): # a POSIX shm object, unlinked at once: the memory of a client
  name = f"/tgs{os.getpid()}"
  fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_RDWR, 0o600)
  _posixshmem.shm_unlink(name)
  os.ftruncate(fd, size)
  return fd
"""

def run_client(tmp:str, sock_path:str, code:str):  # extra/hcq1/remote.py in its own process, its lock file cannot collide with a card session
  return subprocess.run([sys.executable, "-c", PRELUDE + code], env=dict(os.environ, APL_REMOTE_SOCK=sock_path, TMPDIR=tmp, PYTHONPATH=ROOT),
                        capture_output=True, text=True, cwd=ROOT, timeout=300)

class TestAPLRemotePCIDevice(ServerCase): # extra/hcq1/remote.py against the real server.c
  def run_client(self, code:str, **env:str):
    self.start(**env).close()
    r = run_client(self.tmp, self.path, code)
    self.assertEqual(r.returncode, 0, r.stderr[-2000:])

  def test_free_sysmem_returns_the_aperture(self):
    self.run_client("""
a, pages = d.alloc_sysmem(30 << 20)
assert pages[0] == 0x80000000
b, _ = d.alloc_sysmem(30 << 20)  # 60 of the 64 MiB of the mock dext's aperture
assert d.free_sysmem(a)
c, _ = d.alloc_sysmem(30 << 20)  # does not fit unless a was given back
assert d.free_sysmem(b) and d.free_sysmem(c)
""", MOCK_ARENA_MIB="64")

  def test_free_sysmem_only_frees_whole_blocks(self):
    self.run_client("""
a, _ = d.alloc_sysmem(1 << 20)
assert not d.free_sysmem(a.view(0x4000, 0x4000)) and not d.free_sysmem(a.view(0, 0x4000)) and d.can_release  # neither a block nor all of it
a[0x4000:0x4004] = b"live"
assert a[0x4000:0x4004] == b"live"
assert d.free_sysmem(a) and not d.free_sysmem(a)
""")

  def test_reuse_beyond_128_allocations(self):
    self.run_client("""
live = [d.alloc_sysmem(0x4000)[0] for _ in range(128)]
try: d.alloc_sysmem(0x4000); raise SystemExit("129th live block accepted")
except RuntimeError: pass
assert d.free_sysmem(live.pop(77))
for i in range(500):  # 127 blocks stay live, the rest come and go through one index
  v, pages = d.alloc_sysmem(0x4000)
  v[0x2000:0x2004] = i.to_bytes(4, 'little')
  assert bytes(v[0x2000:0x2004]) == i.to_bytes(4, 'little')
  assert d.free_sysmem(v)
for v in live: assert d.free_sysmem(v)
""")

  def test_map_fd(self):
    self.run_client("""
import mmap
fd = shm_object(64 << 20)
mm = mmap.mmap(fd, 64 << 20)
mm[:4] = b"data"
for _ in range(5):  # a full aperture each time
  block, pages = d.map_fd(fd, 0, 64 << 20)
  assert len(pages) == (64 << 20) // 0x1000 and pages[0] == 0x80000000 and pages[-1] == 0x80000000 + (64 << 20) - 0x1000
  assert d.unmap_fd(block)
assert mm[:4] == b"data"  # nothing was written into the client's memory
for off, size in [(0x1000, 0x4000), (0, 0x1000), (0, 0), (0, (64 << 20) + 0x4000), (32 << 20, 64 << 20), (2**64 - 0x4000, 0x8000)]:
  try: d.map_fd(fd, off, size); raise SystemExit(f"{off:#x} {size:#x} accepted")
  except RuntimeError: pass
block, _ = d.map_fd(fd, 32 << 20, 32 << 20)  # the failures left the connection and the indexes alone
assert block == 0 and d.unmap_fd(block) and d.can_release
""", MOCK_ARENA_MIB="64")

  def test_unmap_unknown_block_is_an_error_not_unsupported(self):
    self.run_client("""
try: d.unmap_fd(7); raise SystemExit("an unknown block was released")
except RuntimeError: pass
assert d.can_release
""")

  def test_dext_without_release_dma(self):
    self.run_client("""
block, pages = d.map_fd(shm_object(1 << 20), 0, 1 << 20)  # importing works
assert len(pages) == 256 and d.can_release
assert not d.unmap_fd(block) and not d.can_release  # giving it back does not: asked once
a, _ = d.alloc_sysmem(8 << 20)
assert not d.free_sysmem(a)
a[0:4] = b"live"  # the block is still mapped
assert a[0:4] == b"live" and not d.free_sysmem(a)  # and not asked again
""", MOCK_OLD_DEXT="1")

  def test_refused_release_keeps_the_block(self):
    self.run_client("""
a, _ = d.alloc_sysmem(8 << 20)
a[0x4000:0x4004] = b"live"
try: d.free_sysmem(a); raise SystemExit("refusal not reported")
except RuntimeError: pass
assert d.can_release and a[0x4000:0x4004] == b"live"  # still ours, still mapped
assert d.free_sysmem(a) and not d.free_sysmem(a)
""", MOCK_FAIL_RELEASE="1")

  def test_refused_release_keeps_the_aperture(self):
    self.run_client("""
a, _ = d.alloc_sysmem(64 << 20)
try: d.free_sysmem(a); raise SystemExit("refusal not reported")
except RuntimeError: pass
try: d.alloc_sysmem(1 << 20); raise SystemExit("the DMA space was handed out again")
except RuntimeError: pass
assert d.free_sysmem(a) and d.alloc_sysmem(64 << 20)[1][0] == 0x80000000
""", MOCK_ARENA_MIB="64", MOCK_FAIL_RELEASE="1")

  def test_disconnect_frees_everything(self):
    self.run_client("""
d.alloc_sysmem(32 << 20)
d.map_fd(shm_object(32 << 20), 0, 32 << 20)
os._exit(0)  # no free_sysmem, no unmap_fd, no goodbye
""", MOCK_ARENA_MIB="64")
    c = Client(self.path)  # served once the dead client's connection is cleaned up
    self.assertFalse(shm_exists(self.shm(0)))
    self.assertEqual(c.map_sysmem(64 * MIB)[:2], (0, 0))

class OldServer(threading.Thread):
  """The TinyGPU server from before UNMAP_SYSMEM and MAP_FD: MAP_SYSMEM_FD works, every other command it does not know answers (1, 0, 0)."""
  def __init__(self, path:str):
    super().__init__(daemon=True)
    self.srv, self.unknown = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM), []
    self.srv.bind(path)
    self.srv.listen(1)

  def run(self):
    conn, _ = self.srv.accept()
    with conn:
      while True:
        msg = b""
        while len(msg) < struct.calcsize(REQ):
          chunk, _, _, _ = conn.recvmsg(struct.calcsize(REQ) - len(msg), socket.CMSG_LEN(4))
          if not chunk: return
          msg += chunk
        cmd = msg[0]
        if cmd != MAP_SYSMEM_FD:
          self.unknown.append(cmd)
          conn.sendall(struct.pack(RESP, 1, 0, 0))
          continue
        fd = shm_object(PAGE)
        mm = mmap.mmap(fd, PAGE)
        struct.pack_into('<QQQQ', mm, 0, 0x80000000, PAGE, 0, 0)
        socket.send_fds(conn, [struct.pack(RESP, 0, PAGE, 0)], [fd])
        os.close(fd)

class TestOldServer(unittest.TestCase):
  def test_client_against_a_server_without_release(self):
    if sys.platform != "darwin": self.skipTest("extra/hcq1/remote.py runs on a mac")
    tmp = self.enterContext(tempfile.TemporaryDirectory(prefix="tgold"))
    sock = os.path.join(tmp, "old.sock")
    srv = OldServer(sock)
    srv.start()
    r = run_client(tmp, sock, """
a, pages = d.alloc_sysmem(0x4000)
assert pages == [0x80000000 + i * 0x1000 for i in range(4)] and d.can_release
assert not d.free_sysmem(a) and not d.can_release  # (1, 0, 0): an old server
a[0x2000:0x2004] = b"live"
assert a[0x2000:0x2004] == b"live"  # still mapped
try: d.map_fd(shm_object(0x4000), 0, 0x4000); raise SystemExit("an old server imported memory")
except RuntimeError: pass
assert not d.unmap_fd(0) and not d.free_sysmem(a)  # not asked again
""")
    self.assertEqual(r.returncode, 0, r.stderr[-2000:])
    self.assertEqual(srv.unknown, [UNMAP_SYSMEM, MAP_FD])

if __name__ == "__main__": unittest.main()
