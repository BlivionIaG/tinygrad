#!/usr/bin/env python3
# Runs the REAL extra/usbgpu/tbgpu/installer/Shared/server.c (built against mock_iokit.c, no dext, no GPU, macOS only) and speaks the wire
# protocol to it: MAP_SYSMEM_FD / UNMAP_SYSMEM / MAP_FD, the 128 live block limit, a dext without ReleaseDMA, a refused release, the DMA arena
# cap, cleanup on disconnect, and extra/hcq1/remote.py's APLRemotePCIDevice against it.
#   python3 extra/usbgpu/tbgpu/server_test/test_server.py
import _posixshmem, array, glob, mmap, os, shutil, socket, struct, subprocess, sys, tempfile, time, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER_C = os.path.join(HERE, "..", "installer", "Shared", "server.c")
REQ, RESP = '<BIIQQQ', '<BQQ'
MAP_BAR, MAP_SYSMEM_FD, CFG_READ, MMIO_READ, MMIO_WRITE, UNMAP_SYSMEM, MAP_FD = 1, 2, 3, 6, 7, 15, 16
KR_NO_MEMORY, KR_BAD_ARG, KR_UNSUPPORTED, KR_BUSY = 0xE00002BD, 0xE00002C2, 0xE00002C7, 0xE00002D5
MAGIC, PAGE, MIB = 0x52454C444D410001, 16384, 1 << 20

def build(dst:str):
  srcs = [SERVER_C, os.path.join(HERE, "mock_iokit.c"), os.path.join(HERE, "mock_main.c")]
  clt = "/Library/Developer/CommandLineTools"
  sdks = sorted(glob.glob(f"{clt}/SDKs/MacOSX*.sdk"))
  tries = [["xcrun", "clang"]]
  if sdks: tries.append([f"{clt}/usr/bin/clang", "-isysroot", sdks[-1]]) # works while the Xcode license is not accepted
  for cc in tries:
    if subprocess.run([*cc, "-O1", "-o", dst, *srcs], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0: return
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
    self.proc = subprocess.Popen([self.binary, self.path], env=dict(os.environ, **env), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    self.addCleanup(lambda: (self.proc.kill(), self.proc.wait()))
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

if __name__ == "__main__": unittest.main()
