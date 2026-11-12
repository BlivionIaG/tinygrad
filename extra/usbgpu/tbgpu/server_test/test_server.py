#!/usr/bin/env python3
# Runs the REAL extra/usbgpu/tbgpu/installer/Shared/server.c (built against mock_iokit.c, no dext, no GPU, macOS only) and speaks the wire
# protocol to it: MAP_SYSMEM_FD / UNMAP_SYSMEM / MAP_FD, the 128 live block limit, a dext without ReleaseDMA, a refused release, the DMA arena
# cap, cleanup on disconnect, RESET_LINK (the registry walk to the bridge dexts, the reset ladder, failures, an old dext), and
# extra/hcq1/remote.py's APLRemotePCIDevice against it.
#   python3 extra/usbgpu/tbgpu/server_test/test_server.py
import _posixshmem, array, glob, mmap, os, shutil, socket, struct, subprocess, sys, tempfile, time, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER_C = os.path.join(HERE, "..", "installer", "Shared", "server.c")
REQ, RESP = '<BIIQQQ', '<BQQ'
MAP_BAR, MAP_SYSMEM_FD, CFG_READ, MMIO_READ, MMIO_WRITE, UNMAP_SYSMEM, MAP_FD, RESET_LINK = 1, 2, 3, 6, 7, 15, 16, 17
KR_NO_MEMORY, KR_NO_DEVICE, KR_BAD_ARG, KR_UNSUPPORTED, KR_BUSY = 0xE00002BD, 0xE00002C0, 0xE00002C2, 0xE00002C7, 0xE00002D5
HOT_RESET, WARM_RESET, FUNCTION_RESET, TERMINATE = 1, 2, 0x10, 1
MAGIC, PAGE, MIB = 0x52454C444D410001, 16384, 1 << 20
FEATURES = 18
FEAT_FEATURES, FEAT_READ_1SEND, FEAT_WC_BAR0, FEAT_QOS_SPIN, PROTO_VERSION = 1, 2, 4, 8, 1

def build(dst:str):
  srcs = [os.path.join(HERE, "mock_iokit.c"), os.path.join(HERE, "mock_main.c")]
  clt = "/Library/Developer/CommandLineTools"
  sdks = sorted(glob.glob(f"{clt}/SDKs/MacOSX*.sdk"))
  tries = [["xcrun", "clang"]]
  if sdks: tries.append([f"{clt}/usr/bin/clang", "-isysroot", sdks[-1]]) # works while the Xcode license is not accepted
  def run(cc, args) -> bool: return subprocess.run([*cc, "-O1", *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
  for cc in tries:
    obj = dst + ".server.o"  # server.c alone is built with mock_sys.h: it counts the sends the server makes
    if run(cc, ["-c", "-include", os.path.join(HERE, "mock_sys.h"), "-o", obj, SERVER_C]) and run(cc, ["-o", dst, obj, *srcs]): return
  raise RuntimeError("cannot build server.c with the mock IOKit")

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

  def map_fd(self, fd:int, off:int, size:int) -> tuple[int, int, int, list[tuple[int, int]]]:
    status, r0, r1 = self.rpc(MAP_FD, off, size, fd=fd)
    if status: return status, r0, r1, []
    nseg = struct.unpack('<Q', self.recvall(8))[0]
    raw = struct.unpack(f'<{2 * nseg}Q', self.recvall(16 * nseg))
    return status, r0, r1, list(zip(raw[0::2], raw[1::2]))

  def close(self): self.s.close()

class ServerCase(unittest.TestCase):
  env: dict[str, str] = {}
  binary: str
  tmp: str

  @classmethod
  def setUpClass(cls):
    if sys.platform != "darwin": raise unittest.SkipTest("server.c is macOS only")
    cls.tmp = tempfile.mkdtemp(prefix="tgsrv")
    cls.binary = os.path.join(cls.tmp, "server")
    build(cls.binary)

  @classmethod
  def tearDownClass(cls): shutil.rmtree(cls.tmp, ignore_errors=True)

  def start(self, **env:str) -> Client:
    self.path = os.path.join(self.tmp, f"s{time.monotonic_ns()}.sock")
    p = self.proc = subprocess.Popen([self.binary, self.path], env=dict(os.environ, **env), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    self.addCleanup(lambda: (p.kill(), p.wait()))  # one per server: a test may start several
    self.c = Client(self.path)
    self.addCleanup(self.c.close)
    return self.c

class TestRelease(ServerCase):
  def test_map_table_and_unmap(self):
    c = self.start()
    st, idx, mm, pairs = c.map_sysmem(2 * MIB)
    self.assertEqual((st, idx), (0, 0))
    self.assertEqual(sum(n for _, n in pairs), 2 * MIB)
    mm[:8192] = bytes(8192)  # the table sits at the head of the block, the client zeroes it
    mm[16384:16388] = b"data"
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, 2 * MIB, 1))
    self.assertEqual(bytes(mm[16384:16388]), b"data")  # the client's own mapping survives the unmap
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_BAD_ARG, 1))  # a dead index
    self.assertEqual(c.rpc(UNMAP_SYSMEM, 5000), (1, KR_BAD_ARG, 1))
    st, idx2, _, _ = c.map_sysmem(MIB)
    self.assertEqual((st, idx2), (0, 0))  # the index is reused

  def test_unknown_command_is_the_old_server_answer(self):
    self.assertEqual(self.start().rpc(99), (1, 0, 0))

  def test_live_block_limit_and_churn(self):
    c = self.start()
    live = [c.map_sysmem(PAGE) for _ in range(128)]
    self.assertTrue(all(st == 0 for st, *_ in live))
    self.assertEqual(sorted(i for _, i, _, _ in live), list(range(128)))
    self.assertEqual(c.map_sysmem(PAGE)[0], 1)  # the 129th live block
    self.assertEqual(c.rpc(UNMAP_SYSMEM, 77)[0], 0)
    self.assertEqual(c.map_sysmem(PAGE)[:2], (0, 77))
    self.assertEqual(c.rpc(UNMAP_SYSMEM, 77)[0], 0)
    for _ in range(600):  # far more than 128 blocks in total, 127 of them live all the time
      st, idx, mm, _ = c.map_sysmem(PAGE)
      self.assertEqual((st, idx), (0, 77))
      self.assertEqual(c.rpc(UNMAP_SYSMEM, idx)[:2], (0, PAGE))

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
    st, idx, _, _ = c.map_sysmem(MIB)
    self.assertEqual(st, 0)
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_UNSUPPORTED, 1))  # understood, the dext cannot
    self.assertEqual(c.map_sysmem(MIB)[:2], (0, 1))  # and the block stays live

  def test_refused_release_keeps_the_block(self):
    c = self.start(MOCK_FAIL_RELEASE="1")
    st, idx, _, _ = c.map_sysmem(MIB)
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (1, KR_BUSY, 1))
    self.assertEqual(c.map_sysmem(MIB)[:2], (0, 1))  # still live: the next block gets another index
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, MIB, 1))  # the retry works

  def test_disconnect_frees_everything(self):
    c = self.start(MOCK_ARENA_MIB="64")
    self.assertEqual(c.map_sysmem(64 * MIB)[0], 0)
    c.close()
    time.sleep(0.2)
    c2 = Client(self.path)
    self.assertEqual(c2.map_sysmem(64 * MIB)[:2], (0, 0))

  def test_mmio_still_works(self):
    c = self.start()
    st, size, _ = c.rpc(MAP_BAR, bar=0)
    self.assertEqual((st, size >= 0), (0, True))

class TestMapFd(ServerCase):
  def store(self, size:int) -> tuple[int, mmap.mmap]: # a POSIX shm object the test owns, like the expert store of a client
    name = f"/tgs{os.getpid() % 10**6}x{time.monotonic_ns() % 10**9}"
    fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_RDWR, 0o600)
    _posixshmem.shm_unlink(name)
    os.ftruncate(fd, size)
    self.addCleanup(os.close, fd)
    return fd, mmap.mmap(fd, size)

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

  def test_bad_arguments(self):
    c = self.start()
    fd, _ = self.store(4 * MIB)
    self.assertEqual(c.map_fd(fd, 100, 2 * PAGE)[0], 1)  # offset not a multiple of 16 KiB
    self.assertEqual(c.map_fd(fd, 0, 100)[0], 1)  # size neither
    self.assertEqual(c.map_fd(fd, 0, 0)[0], 1)
    self.assertEqual(c.rpc(MAP_FD, 0, PAGE)[0], 1)  # no fd with the request
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))  # the connection is fine

  def test_fd_with_another_command_is_dropped(self):
    c = self.start()
    fd, _ = self.store(MIB)
    self.assertEqual(c.rpc(CFG_READ, 0, 2, fd=fd)[:2], (0, 0x1002))

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

class TestResetLink(ServerCase):
  def start(self, **env:str) -> Client:
    self.log = os.path.join(self.tmp, f"log{time.monotonic_ns()}")
    return super().start(MOCK_LOG=self.log, **env)

  def calls(self) -> list[tuple[int, int, int]]: # (level, type, options) of every selector 5 the mock saw
    if not os.path.exists(self.log): return []
    with open(self.log) as f: return [tuple(map(int, l.split())) for l in f]  # type: ignore[misc]

  def idle(self, c:Client): # the server holds nothing (no bridge connection, no registry object) and still serves
    self.assertEqual(c.rpc(CFG_READ, 0xF000, 4)[:2], (0, 0), "a bridge connection is still open")
    self.assertEqual(c.rpc(CFG_READ, 0xF004, 4)[:2], (0, 0), "a registry object was not released")
    self.assertEqual(c.rpc(CFG_READ, 0xF008, 4)[:2], (0, 0), "a release or close that matched nothing")
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))

  def test_query(self):
    for n in (0, 1, 2, 4):
      with self.subTest(bridges=n):
        c = self.start(MOCK_BRIDGES=str(n))
        self.assertEqual(c.rpc(RESET_LINK, 0, 0, 0), (0, n, 1))
        self.assertEqual(c.rpc(RESET_LINK, 3, 0, 1), (0, n, 1))  # level and options mean nothing to a query
        self.assertEqual(self.calls(), [])  # nothing was reset, no bridge user client was even opened
        self.idle(c)
        c.close()
    c = self.start()  # four bridge dexts by default
    self.assertEqual(c.rpc(RESET_LINK), (0, 4, 1))

  def test_level0(self):
    for n in (0, 4):  # the GPU's own nub needs no bridge dext
      c = self.start(MOCK_BRIDGES=str(n))
      for typ in (HOT_RESET, WARM_RESET, FUNCTION_RESET): self.assertEqual(c.rpc(RESET_LINK, 0, typ, 0), (0, 0, 1))
      self.assertEqual(self.calls(), [(0, HOT_RESET, 0), (0, WARM_RESET, 0), (0, FUNCTION_RESET, 0)])
      self.idle(c)
      c.close()

  def test_each_level_reaches_its_bridge(self):
    c = self.start()
    for k in range(1, 5):
      self.assertEqual(c.rpc(RESET_LINK, k, HOT_RESET, 0), (0, 0, 1))
      self.assertEqual(self.calls()[-1], (k, HOT_RESET, 0))
      self.idle(c)  # closed again right after the call
    self.assertEqual(c.rpc(RESET_LINK, 2, WARM_RESET, 0), (0, 0, 1))
    self.assertEqual(c.rpc(RESET_LINK, 0, HOT_RESET, 0), (0, 0, 1))
    self.assertEqual(self.calls(), [(1, 1, 0), (2, 1, 0), (3, 1, 0), (4, 1, 0), (2, 2, 0), (0, 1, 0)])
    self.idle(c)

  def test_level_without_bridge_dext(self):
    c = self.start()
    for k in (5, 6, 31, 32, 33, 1 << 32, 1 << 40): self.assertEqual(c.rpc(RESET_LINK, k, HOT_RESET, 0), (1, KR_NO_DEVICE, 1))
    self.assertEqual(self.calls(), [])
    self.idle(c)
    c.close()
    c = self.start(MOCK_BRIDGES="2")  # bridge dexts only on the two nubs nearest the GPU
    self.assertEqual([c.rpc(RESET_LINK, k, HOT_RESET, 0) for k in range(1, 5)], [(0, 0, 1), (0, 0, 1), (1, KR_NO_DEVICE, 1), (1, KR_NO_DEVICE, 1)])
    self.assertEqual(self.calls(), [(1, 1, 0), (2, 1, 0)])
    self.idle(c)
    c.close()
    c = self.start(MOCK_BRIDGES="0")  # a dext without the bridge personality (the release build)
    self.assertEqual(c.rpc(RESET_LINK, 1, HOT_RESET, 0), (1, KR_NO_DEVICE, 1))
    self.assertEqual(c.rpc(RESET_LINK, 0, HOT_RESET, 0), (0, 0, 1))
    self.idle(c)

  def test_failures(self):
    c = self.start(MOCK_RESET_KR=hex(KR_BUSY))
    for k in (0, 1, 4): self.assertEqual(c.rpc(RESET_LINK, k, HOT_RESET, 0), (1, KR_BUSY, 1))  # what Reset returned, unchanged
    self.assertEqual(self.calls(), [(0, 1, 0), (1, 1, 0), (4, 1, 0)])
    self.idle(c)
    c.close()
    c = self.start(MOCK_RESET_KR_LEVEL2=hex(KR_BAD_ARG))
    self.assertEqual([c.rpc(RESET_LINK, k, HOT_RESET, 0) for k in range(4)], [(0, 0, 1), (0, 0, 1), (1, KR_BAD_ARG, 1), (0, 0, 1)])
    self.idle(c)
    c.close()
    c = self.start(MOCK_OPEN_KR_LEVEL3="0xE00002CD")  # the bridge user client does not open: its IOServiceOpen error
    self.assertEqual(c.rpc(RESET_LINK, 3, HOT_RESET, 0), (1, 0xE00002CD, 1))
    self.assertEqual(c.rpc(RESET_LINK, 4, HOT_RESET, 0), (0, 0, 1))
    self.assertEqual(self.calls(), [(4, 1, 0)])
    self.idle(c)
    c.close()
    c = self.start()
    for typ, opt in ((1 << 32, 0), (1, 1 << 32), (1 << 63, 1 << 63)): self.assertEqual(c.rpc(RESET_LINK, 0, typ, opt), (1, KR_BAD_ARG, 1))  # the dext takes 32 bits
    self.assertEqual(self.calls(), [])
    self.idle(c)

  def test_type_zero_never_resets(self):
    c = self.start()
    for k in range(0, 8):
      for opt in (0, TERMINATE, 0xFF): self.assertEqual(c.rpc(RESET_LINK, k, 0, opt), (0, 4, 1))
    self.assertEqual(self.calls(), [])
    self.idle(c)

  def test_terminate_option_is_passed_through(self):
    c = self.start()
    for k in (0, 3): self.assertEqual(c.rpc(RESET_LINK, k, HOT_RESET, TERMINATE), (0, 0, 1))
    self.assertEqual(c.rpc(RESET_LINK, 1, WARM_RESET, 0xA5), (0, 0, 1))
    self.assertEqual(self.calls(), [(0, 1, 1), (3, 1, 1), (1, 2, 0xA5)])
    self.idle(c)  # the mock does not terminate anything here: the server keeps serving

  def test_terminate_notification_during_the_reset_exits_after_the_answer(self):
    for k in (0, 2):
      c = self.start(MOCK_TERMINATE_NOTIFY="1")  # the dext terminates while Reset runs: the server answers first, then exits
      self.assertEqual(c.rpc(RESET_LINK, k, HOT_RESET, TERMINATE), (0, 0, 1))
      self.assertEqual(c.s.recv(1), b"")  # the connection closes
      self.assertEqual(self.proc.wait(10), 0)
      self.assertEqual(self.calls(), [(k, 1, 1)])
      c.close()
    c = self.start(MOCK_TERMINATE_NOTIFY="1")  # no terminate option, no termination: still alive
    self.assertEqual(c.rpc(RESET_LINK, 0, HOT_RESET, 0), (0, 0, 1))
    self.idle(c)

  def test_old_dext(self):
    c = self.start(MOCK_OLD_DEXT="1")  # a GPU dext without ResetLink and without bridge dexts
    self.assertEqual(c.rpc(RESET_LINK), (0, 0, 1))
    self.assertEqual(c.rpc(RESET_LINK, 0, HOT_RESET, 0), (1, KR_UNSUPPORTED, 1))
    self.assertEqual(c.rpc(RESET_LINK, 1, HOT_RESET, 0), (1, KR_NO_DEVICE, 1))
    self.assertEqual(self.calls(), [])
    self.idle(c)

  def test_after_other_traffic(self):
    c = self.start()
    st, idx, mm, _ = c.map_sysmem(2 * MIB)
    self.assertEqual((st, idx), (0, 0))
    self.assertEqual(c.rpc(MAP_BAR, bar=0)[0], 0)
    self.assertEqual(c.rpc(RESET_LINK, 1, HOT_RESET, 0), (0, 0, 1))
    self.assertEqual(c.rpc(UNMAP_SYSMEM, idx), (0, 2 * MIB, 1))  # the DMA state is untouched by a reset request
    self.assertEqual(c.rpc(RESET_LINK, 0, HOT_RESET, 0), (0, 0, 1))
    self.assertEqual(c.map_sysmem(MIB)[:2], (0, 0))
    self.assertEqual(c.rpc(RESET_LINK), (0, 4, 1))
    self.idle(c)
    c.close()
    c = self.start()  # a new client after the old one left
    self.assertEqual(c.rpc(RESET_LINK, 4, HOT_RESET, 0), (0, 0, 1))
    self.idle(c)

PRELUDE = """
import _posixshmem, os
from extra.hcq1.remote import APLRemotePCIDevice
APLRemotePCIDevice.ensure_app = classmethod(lambda cls: None) # the TinyGPU.app is not needed, the server under test is already running
d = APLRemotePCIDevice("AM", "usb4")
"""

class TestAPLRemotePCIDevice(ServerCase): # extra/hcq1/remote.py against the real server.c
  def run_client(self, code:str, **env:str):
    self.start(**env).close()
    root = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
    r = subprocess.run([sys.executable, "-c", PRELUDE + code], env=dict(os.environ, APL_REMOTE_SOCK=self.path, TMPDIR=self.tmp, PYTHONPATH=root),
                       capture_output=True, text=True, cwd=root) # own TMPDIR: the client's lock file cannot collide with a card session
    self.assertEqual(r.returncode, 0, r.stderr[-2000:])

  def test_free_sysmem_returns_the_aperture(self):
    self.run_client("""
a, pages = d.alloc_sysmem(30 << 20)
assert pages[0] == 0x80000000 and d.blocks == {a.addr: 0}
b, _ = d.alloc_sysmem(30 << 20)  # 60 of the 64 MiB of the mock dext's aperture
assert d.free_sysmem(a) and d.can_release and list(d.blocks.values()) == [1]
c, _ = d.alloc_sysmem(30 << 20)  # does not fit unless a was given back; takes its index
assert sorted(d.blocks.values()) == [0, 1] and d.free_sysmem(b) and d.free_sysmem(c) and not d.blocks
""", MOCK_ARENA_MIB="64")

  def test_map_fd(self):
    self.run_client("""
import mmap
name = f"/tgs{os.getpid()}"
fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_RDWR, 0o600)
_posixshmem.shm_unlink(name)
os.ftruncate(fd, 64 << 20)
mm = mmap.mmap(fd, 64 << 20)
mm[:4] = b"data"
for _ in range(5):  # a full aperture each time
  block, pages = d.map_fd(fd, 0, 64 << 20)
  assert len(pages) == (64 << 20) // 0x1000 and pages[0] == 0x80000000
  assert d.unmap_fd(block)
assert mm[:4] == b"data"  # nothing was written into the client's memory
try: d.map_fd(fd, 0x1000, 0x4000); raise SystemExit("unaligned offset accepted")
except RuntimeError: pass
""", MOCK_ARENA_MIB="64")

  def test_dext_without_release_dma(self):
    self.run_client("""
a, _ = d.alloc_sysmem(8 << 20)
assert not d.free_sysmem(a) and not d.can_release and a.addr in d.blocks
a[0:4] = b"live"  # the block is still mapped
assert a[0:4] == b"live" and not d.free_sysmem(a)  # and not asked again
""", MOCK_OLD_DEXT="1")

  def test_refused_release_keeps_the_block(self):
    self.run_client("""
a, _ = d.alloc_sysmem(8 << 20)
try: d.free_sysmem(a); raise SystemExit("refusal not reported")
except RuntimeError as e: assert "refused" in str(e)
assert a.addr in d.blocks and d.can_release and d.free_sysmem(a)
""", MOCK_FAIL_RELEASE="1")


class TestFeatures(ServerCase):
  def mmio(self, c:Client, bar:int, off:int, n:int) -> bytes:
    c.s.sendall(struct.pack(REQ, MMIO_READ, 0, bar, off, n, 0))
    st, r0, _ = struct.unpack(RESP, c.recvall(struct.calcsize(RESP)))
    self.assertEqual((st, r0), (0, n))
    return c.recvall(n)

  def write(self, c:Client, bar:int, off:int, data:bytes):
    c.s.sendall(struct.pack(REQ, MMIO_WRITE, 0, bar, off, len(data), 0) + data)

  def test_features_answer(self):
    st, reg, f = self.start().rpc(FEATURES, PROTO_VERSION)
    self.assertEqual((st, reg, f >> 32), (0, 0x100000a5c, 1))
    self.assertEqual(f & 0xFFFFFFFF, FEAT_FEATURES | FEAT_READ_1SEND | FEAT_WC_BAR0 | FEAT_QOS_SPIN)

  def test_features_registry_id_unknown(self):
    self.assertEqual(self.start(MOCK_REG_ID="0").rpc(FEATURES, PROTO_VERSION)[1], 0)

  def test_features_without_wc_in_the_dext(self):
    for env in ({"MOCK_NO_WC": "1"}, {"MOCK_OLD_DEXT": "1"}):  # a dext that does not know selector 6
      st, _, f = self.start(**env).rpc(FEATURES, PROTO_VERSION)
      self.assertEqual((st, f & 0xFFFFFFFF), (0, FEAT_FEATURES | FEAT_READ_1SEND | FEAT_QOS_SPIN))

  def test_old_server_answer_means_no_features(self):  # what the client sees from a server that predates 18 (and what a command it does not know gets)
    self.assertEqual(self.start().rpc(99), (1, 0, 0))

  def test_features_do_not_disturb_the_stream(self):
    c = self.start()
    c.rpc(FEATURES, PROTO_VERSION)
    self.assertEqual(c.rpc(MAP_BAR, bar=0)[0], 0)
    self.write(c, 0, 0x40, b"\x01\x02\x03\x04")
    self.assertEqual(self.mmio(c, 0, 0x40, 4), b"\x01\x02\x03\x04")

  def test_read_answer_is_one_send(self):
    c = self.start()
    c.rpc(MAP_BAR, bar=2)
    self.write(c, 2, 0, bytes(range(256)))
    before = c.rpc(CFG_READ, 0xF00C, 4)[1]
    self.assertEqual(self.mmio(c, 2, 0, 256), bytes(range(256)))
    after = c.rpc(CFG_READ, 0xF00C, 4)[1]
    self.assertEqual(after - before, 2)  # the read's answer and the answer of the first CFG_READ: the header and the data went in one sendmsg

  def test_big_read_survives_partial_sends(self):
    c = self.start()
    c.rpc(MAP_BAR, bar=0)
    pattern = bytes((i * 7) & 0xFF for i in range(1 << 20))
    self.write(c, 0, 0, pattern)
    for _ in range(4): self.assertEqual(self.mmio(c, 0, 0, 1 << 20), pattern)

  def test_qos_user_interactive(self):
    self.assertEqual(self.start().rpc(CFG_READ, 0xF010, 4)[1], 0x21)  # QOS_CLASS_USER_INTERACTIVE

  def test_spin_keeps_the_stream_and_eof_working(self):
    c = self.start(TINYGPU_SPIN_US="200000")
    c.rpc(MAP_BAR, bar=0)
    for i in range(200):
      self.write(c, 0, 4 * i, struct.pack('<I', i))
      self.assertEqual(struct.unpack('<I', self.mmio(c, 0, 4 * i, 4))[0], i)
    c.close()  # EOF while the server spins
    c2 = Client(self.path)
    self.addCleanup(c2.close)
    self.assertEqual(c2.rpc(MAP_BAR, bar=0)[0], 0)
    self.assertEqual(c2.rpc(CFG_READ, 0, 2)[1], 0x1002)

if __name__ == "__main__": unittest.main()
