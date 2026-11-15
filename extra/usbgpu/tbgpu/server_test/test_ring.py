#!/usr/bin/env python3
# Exercise the real server, C stress client and opt-in Python consumer without IOKit, a dext or a GPU (macOS only).
# Run: python3 -m pytest -n12 extra/usbgpu/tbgpu/server_test/test_ring.py
# TINYGPU_TEST_SERVER_C selects an older server source for the regression red gate.
import contextlib, ctypes, functools, glob, mmap, os, random, shutil, socket, struct, subprocess, sys, tempfile, threading, time, unittest, atexit
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
SERVER_C = os.environ.get("TINYGPU_TEST_SERVER_C") or os.path.join(HERE, "..", "installer", "Shared", "server.c")
REQ, RESP = '<BIIQQQ', '<BQQ'
MAP_BAR, CFG_READ, CFG_WRITE, RESET, MMIO_READ, MMIO_WRITE, RESIZE_BAR, FEATURES, RING_SETUP, RING_KICK = 1, 3, 4, 5, 6, 7, 11, 18, 19, 20
MAP_SYSMEM_FD = 2
KR_BAD_ARG, KR_BUSY = 0xE00002C2, 0xE00002D5
FEAT_FEATURES, FEAT_RING, PROTO_VERSION = 1, 16, 1
RING_MAGIC, HDR = 0x5447524E47000001, 16384
RING_REQ_HEAD, RING_REQ_TAIL, RING_RSP_HEAD, RING_RSP_TAIL, RING_SRV_STATE = 128, 256, 384, 512, 640
BULK = 64 << 20  # the server's bulk buffer

def wait_until(cond, timeout:float=10.0, what:str="the condition"):
  end = time.monotonic() + timeout
  while not cond():
    if time.monotonic() > end: raise TimeoutError(f"timed out waiting for {what}")
    time.sleep(0.002)

def ring_caps(m, caps:dict[str, int]):  # the sizes of the rings TinyGPURing.open will ask for
  return patch.multiple(m.TinyGPURing, **caps) if caps else contextlib.nullcontext()

def compile_c(args:list[str]) -> str|None:  # None: built, else what the compiler said
  clt = "/Library/Developer/CommandLineTools"
  sdks = sorted(glob.glob(f"{clt}/SDKs/MacOSX*.sdk"))
  tries = [["xcrun", "clang"]]
  if sdks: tries.append([f"{clt}/usr/bin/clang", "-isysroot", sdks[-1]])  # works while the Xcode license is not accepted
  err = ""
  for cc in tries:
    r = subprocess.run([*cc, "-O2", *args], capture_output=True, text=True)
    if r.returncode == 0: return None
    err = r.stderr[-3000:]
  return err

@functools.cache
def binary(name:str) -> str:  # built once per process, in its own directory
  tmp = tempfile.mkdtemp(prefix="tgring_bin")
  atexit.register(shutil.rmtree, tmp, ignore_errors=True)
  dst = os.path.join(tmp, name)
  srcs = ([SERVER_C, os.path.join(HERE, "mock_iokit.c"), os.path.join(HERE, "mock_main.c")] if name == "server"
          else [os.path.join(HERE, "ring_stress.c")])
  if (err := compile_c(["-o", dst, *srcs])) is not None: raise RuntimeError(f"cannot build {name}: {err}")
  return dst

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

  def read(self, off:int, n:int, bar=0) -> bytes:  # MMIO_READ on the socket: the legacy path
    self.s.sendall(struct.pack(REQ, MMIO_READ, 0, bar, off, n, 0))
    assert struct.unpack(RESP, self.recvall(17))[:2] == (0, n)
    return self.recvall(n)

  def close(self): self.s.close()

class ServerCase(unittest.TestCase):
  tmp: str

  @classmethod
  def setUpClass(cls):
    if sys.platform != "darwin": raise unittest.SkipTest("server.c is macOS only")
    cls.tmp = tempfile.mkdtemp(prefix="tgring")
    binary("server")

  @classmethod
  def tearDownClass(cls): shutil.rmtree(cls.tmp, ignore_errors=True)

  def start(self, **env:str) -> Client:
    self.path = os.path.join(self.tmp, f"s{os.urandom(4).hex()}.sock")  # sun_path holds 104 bytes
    p = self.proc = subprocess.Popen([binary("server"), self.path], env=dict(os.environ, **env), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    self.addCleanup(lambda: (p.kill(), p.wait()))  # one per server: a test may start several
    self.c = Client(self.path)
    self.addCleanup(self.c.close)
    return self.c

  def reconnect(self) -> Client:  # the server serves one client at a time: this one is served once the previous one is gone
    c = Client(self.path)
    self.addCleanup(c.close)
    return c

  def assert_dropped(self, c:Client):
    def gone() -> bool:
      try: c.rpc(CFG_READ, 0, 2)
      except OSError: return True  # ConnectionError, BrokenPipeError, a timeout
      return False
    wait_until(gone, 20, "the server to drop the client")

# Wire-level client that can deliberately violate the protocol.

class Ring:
  """Allow malformed setup headers. Shared words use aligned 64-bit accesses with fences."""
  def __init__(self, c:Client, req_cap=1 << 16, rsp_cap=1 << 16, size:int|None=None, magic=RING_MAGIC, version=1, data_off=HDR):
    import _posixshmem
    self.c, self.req_cap, self.rsp_cap, self.kicks = c, req_cap, rsp_cap, 0
    self.bytes = HDR + req_cap + rsp_cap
    name = f"/tgr{os.getpid()}_{os.urandom(4).hex()}"
    self.fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    _posixshmem.shm_unlink(name)
    os.ftruncate(self.fd, size or self.bytes)
    self.mm = mmap.mmap(self.fd, size or self.bytes)
    struct.pack_into('<QIIII', self.mm, 0, magic, version, req_cap, rsp_cap, data_off)
    self.head = self.tail = 0  # our request head, our response tail
    self.words = {off: ctypes.c_uint64.from_buffer(self.mm, off)
                  for off in (RING_REQ_HEAD, RING_REQ_TAIL, RING_RSP_HEAD, RING_RSP_TAIL, RING_SRV_STATE)}
    self.fence = ctypes.CDLL(None).OSMemoryBarrier  # a full dmb: python's own stores and loads are not ordered against another process

  def setup(self, fd:int|None=None) -> tuple[int, int, int]: return self.c.rpc(RING_SETUP, self.bytes, fd=self.fd if fd is None else fd)
  def word(self, off:int) -> int: return self.words[off].value
  def srv_state(self) -> int: return self.word(RING_SRV_STATE)
  def close(self):
    self.words = {}  # the views pin the mmap
    self.mm.close()
    os.close(self.fd)

  def put(self, data:bytes):
    mv, h, t0 = memoryview(data), self.head, time.monotonic()
    while len(mv):
      space = self.req_cap - (h - self.word(RING_REQ_TAIL))
      if not space:
        self.publish(h)
        if time.monotonic() - t0 > 20: raise TimeoutError("request ring stuck")
        continue
      self.fence()
      k = min(space, len(mv))
      pos = h & (self.req_cap - 1)
      first = min(k, self.req_cap - pos)
      self.mm[HDR + pos:HDR + pos + first] = mv[:first]
      self.mm[HDR:HDR + k - first] = mv[first:k]
      h, mv, t0 = h + k, mv[k:], time.monotonic()
    self.publish(h)

  def publish(self, h:int):
    self.head = h
    self.fence()  # the data before the head (release), the head before the look at srv_state (Dekker)
    self.words[RING_REQ_HEAD].value = h
    self.fence()
    if self.srv_state() == 1:  # the server sleeps: one kick
      self.words[RING_SRV_STATE].value = 2
      self.kicks += 1
      self.c.s.sendall(struct.pack(REQ, RING_KICK, 0, 0, 0, 0, 0))

  def get(self, n:int) -> bytes:
    out, t0 = [], time.monotonic()
    while n:
      avail = self.word(RING_RSP_HEAD) - self.tail
      if not avail:
        if time.monotonic() - t0 > 20: raise TimeoutError("no answer on the response ring")
        continue
      self.fence()
      k = min(avail, n)
      pos = self.tail & (self.rsp_cap - 1)
      first = min(k, self.rsp_cap - pos)
      base = HDR + self.req_cap
      out += [bytes(self.mm[base + pos:base + pos + first]), bytes(self.mm[base:base + k - first])]
      self.tail += k
      n -= k
      self.fence()
      self.words[RING_RSP_TAIL].value = self.tail
      t0 = time.monotonic()
    return b"".join(out)

  def answer(self, cmd:int, a0=0, a1=0, a2=0, bar=0, payload=b"") -> tuple[int, int, int]:
    self.put(struct.pack(REQ, cmd, 0, bar, a0, a1, a2) + payload)
    return struct.unpack(RESP, self.get(struct.calcsize(RESP)))

  def write(self, bar:int, off:int, data:bytes): self.put(struct.pack(REQ, MMIO_WRITE, 0, bar, off, len(data), 0) + data)
  def read(self, bar:int, off:int, n:int) -> bytes:
    st, r0, _ = self.answer(MMIO_READ, off, n, bar=bar)
    assert (st, r0) == (0, n), (st, r0)
    return self.get(n)

class TestRingProtocol(ServerCase):
  def ring(self, c:Client, **kw) -> Ring:
    r = Ring(c, **kw)
    self.addCleanup(r.close)
    return r

  def up(self, **kw) -> tuple[Client, Ring]:
    c = self.start()
    r = self.ring(c, **kw)
    self.assertEqual(r.setup(), (0, r.bytes, 1))
    self.assertEqual(c.rpc(MAP_BAR, bar=0)[0], 0)
    return c, r

  def asleep(self, r:Ring): wait_until(lambda: r.srv_state() == 1, 10, "the server to go to sleep")

  def test_features(self):
    c = self.start()
    self.assertEqual(c.rpc(FEATURES, PROTO_VERSION), (0, 0, PROTO_VERSION << 32 | FEAT_FEATURES | FEAT_RING))
    self.assertEqual(c.rpc(FEATURES, 0)[2] >> 32, PROTO_VERSION)  # whatever version the client says, it learns the server's
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))  # the stream goes on
    self.assertEqual(c.rpc(99), (1, 0, 0))  # an unknown command is answered the way an old server answers FEATURES

  def test_roundtrip_and_ordering(self):
    c, r = self.up()
    self.assertEqual(r.answer(CFG_READ, 0, 2), (0, 0x1002, 0))
    for i in range(300):
      d = struct.pack('<I', i * 77)
      r.write(0, 4 * i, d)
      self.assertEqual(r.read(0, 4 * i, 4), d)  # a read sees every write posted before it
    r.write(0, 0x1000, b"abcdefgh")
    self.assertEqual(r.read(0, 0x1000, 8), b"abcdefgh")

  def test_config_commands_on_the_ring(self):
    c, r = self.up()
    self.assertEqual(r.answer(CFG_WRITE, 0x10, 4, 0xdead), (0, 0, 0))
    self.assertEqual(r.answer(RESIZE_BAR, bar=0), (0, 0, 0))
    self.assertEqual(r.answer(RESET), (0, 0, 0))
    self.assertEqual(r.answer(CFG_READ, 0, 2), (0, 0x1002, 0))

  def test_payloads_longer_than_the_ring(self):
    c = self.start()
    r = self.ring(c, req_cap=4096, rsp_cap=4096)
    self.assertEqual(r.setup()[0], 0)
    c.rpc(MAP_BAR, bar=0)
    pattern = bytes((i * 31 + (i >> 8)) & 0xFF for i in range(1 << 20))
    r.write(0, 0, pattern)
    self.assertEqual(r.read(0, 0, 1 << 20), pattern)

  def test_requests_straddle_the_end_of_a_small_ring(self):
    c = self.start()
    r = self.ring(c, req_cap=4096, rsp_cap=4096)  # 33-byte headers and answers of odd lengths: every offset of the ring gets crossed
    self.assertEqual(r.setup()[0], 0)
    c.rpc(MAP_BAR, bar=0)
    rng, model = random.Random(7), bytearray(1 << 16)
    for _ in range(800):
      off, n = rng.randrange(0, (len(model) - 1024) // 4) * 4, rng.randrange(1, 900)
      if rng.random() < 0.5:
        data = rng.randbytes(n)
        r.write(0, off, data)
        model[off:off + n] = data
      else: self.assertEqual(r.read(0, off, n), bytes(model[off:off + n]))

  def test_unsupported_commands_are_refused_on_the_ring(self):
    c, r = self.up()
    for cmd in (MAP_BAR, FEATURES, MAP_SYSMEM_FD, RING_SETUP, RING_KICK, 99): self.assertEqual(r.answer(cmd), (1, 0, 0), cmd)
    self.assertEqual(r.read(0, 0, 4), bytes(4))

  def test_malformed_requests_are_refused_or_dropped_without_touching_memory(self):
    c, r = self.up()
    for off, n in (((1 << 64) - 8, 16), ((1 << 64) - 1, 2), (1 << 20, 1), ((1 << 20) - 4, 8), (0, 65 << 20), (1 << 30, 4)):
      self.assertEqual(r.answer(MMIO_READ, off, n), (1, 0, 0), (off, n))
    for bar in (1, 5, 6, 256, 1 << 31): self.assertEqual(r.answer(MMIO_READ, 0, 4, bar=bar), (1, 0, 0), bar)
    r.write(0, 0, b"keep")
    for bar, off, data in ((0, (1 << 64) - 4, b"WRAPWRAP"), (0, 1 << 20, b"PASTEND!"), (1, 0, b"no bar!!!")):
      r.write(bar, off, data)  # invalid writes are dropped, but their payload is consumed
    self.assertEqual(r.read(0, 0, 8), b"keep" + bytes(4))
    self.assertEqual(r.answer(CFG_READ, 0, 2), (0, 0x1002, 0))  # still in step

  def test_oversized_write_drops_the_client(self):
    c, r = self.up()
    r.put(struct.pack(REQ, MMIO_WRITE, 0, 0, 0, BULK + 1, 0))  # nothing that follows could be told from the next request
    self.assert_dropped(c)
    c2 = self.reconnect()  # the server itself is fine
    self.assertEqual(c2.rpc(MAP_BAR, bar=0)[0], 0)

  def test_header_is_read_once(self):
    c, r = self.up()
    struct.pack_into('<QIIII', r.mm, 0, 0, 7, 3, 5, 99)  # what the client does to the object afterwards changes nothing for the server
    r.write(0, 0, b"still")
    self.assertEqual(r.read(0, 0, 5), b"still")

  def test_socket_request_sees_earlier_ring_writes(self):
    c, r = self.up()
    r.write(0, 0x200, b"wxyz")
    self.assertEqual(c.read(0x200, 4), b"wxyz")  # the legacy path, right behind the ring entry
    self.asleep(r)
    r.write(0, 0x204, b"1234")  # the server sleeps: a kick, then the request
    self.assertEqual(c.read(0x200, 8), b"wxyz1234")
    self.assertEqual(c.rpc(FEATURES, PROTO_VERSION)[2] >> 32, 1)  # and the socket goes on working

  def test_kick_without_a_ring_is_dropped(self):
    c = self.start()
    c.s.sendall(struct.pack(REQ, RING_KICK, 0, 0, 0, 0, 0))
    self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))  # no stray answer in front of this one

  def test_setup_refusals(self):
    c = self.start()
    good = dict(req_cap=4096, rsp_cap=4096)
    cases = [
      (dict(magic=1), KR_BAD_ARG), (dict(version=2), KR_BAD_ARG), (dict(data_off=4096), KR_BAD_ARG),
      (dict(req_cap=5000, rsp_cap=4096), KR_BAD_ARG), (dict(req_cap=2048, rsp_cap=8192), KR_BAD_ARG), (dict(req_cap=4096, rsp_cap=0), KR_BAD_ARG),
      (dict(size=16384), KR_BAD_ARG),  # the object is smaller than the ring says
    ]
    for kw, kr in cases:
      r = self.ring(c, **{**good, **kw})
      self.assertEqual(r.setup(), (1, kr, 1), kw)
      self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))  # the connection is fine
    r = self.ring(c, **good)
    struct.pack_into('<Q', r.mm, RING_REQ_HEAD, 5)  # not fresh
    self.assertEqual(r.setup(), (1, KR_BAD_ARG, 1))
    self.assertEqual(c.rpc(RING_SETUP, 20000), (1, KR_BAD_ARG, 1))  # no fd
    self.assertEqual(c.rpc(RING_SETUP, (256 << 20) + 1, fd=r.fd), (1, KR_BAD_ARG, 1))  # more than the server maps
    fd = os.open("/dev/null", os.O_RDWR)
    self.addCleanup(os.close, fd)
    self.assertEqual(c.rpc(RING_SETUP, 30000, fd=fd), (1, KR_BAD_ARG, 1))  # an object with nothing in it
    r = self.ring(c, **good)
    self.assertEqual(r.setup(), (0, r.bytes, 1))
    r2 = self.ring(c, **good)
    self.assertEqual(r2.setup(), (1, KR_BUSY, 1))  # one ring per connection
    self.assertEqual(r.answer(CFG_READ, 0, 2), (0, 0x1002, 0))

  def test_a_new_connection_can_set_up_again(self):
    c, r = self.up()
    r.write(0, 0, b"keep")
    c.close()
    c2 = self.reconnect()
    r2 = self.ring(c2)
    self.assertEqual(r2.setup(), (0, r2.bytes, 1))
    c2.rpc(MAP_BAR, bar=0)
    self.assertEqual(r2.read(0, 0, 4), b"keep")  # the posted write landed before the first connection went

  def test_posted_writes_survive_eof(self):
    c, r = self.up()
    self.asleep(r)
    r.write(0, 0x300, b"EOF!" * 100)
    c.close()  # the kick and the EOF arrive together
    c2 = self.reconnect()
    r2 = self.ring(c2)
    self.assertEqual(r2.setup()[0], 0)
    c2.rpc(MAP_BAR, bar=0)
    self.assertEqual(r2.read(0, 0x300, 400), b"EOF!" * 100)

  def test_sleep_and_kick(self):
    c, r = self.up()
    for i in range(20):
      self.asleep(r)
      data = struct.pack('<I', i)
      r.put(struct.pack(REQ, MMIO_WRITE, 0, 0, 0x400, 4, 0) + data + struct.pack(REQ, MMIO_READ, 0, 0, 0x400, 4, 0))  # one publish: exactly one kick
      self.assertEqual(struct.unpack(RESP, r.get(17))[:2], (0, 4))
      self.assertEqual(r.get(4), data)
      self.assertEqual(r.kicks, i + 1)

  def test_counters_that_make_no_sense_drop_the_client(self):
    def head_ahead(r:Ring): r.words[RING_REQ_HEAD].value = 3 * (1 << 16)
    def head_behind(r:Ring):
      r.write(0, 0, bytes(4))
      r.read(0, 0, 4)
      r.words[RING_REQ_HEAD].value = 0
      # The next socket request wakes the server, which finds an impossible head behind the tail.
    def tail_absurd(r:Ring):
      r.words[RING_RSP_TAIL].value = 1 << 40
      r.write(0, 0, bytes(4))
      r.put(struct.pack(REQ, MMIO_READ, 0, 0, 0, 4, 0))  # the server wants to answer
    for corrupt in (head_ahead, head_behind, tail_absurd):
      c, r = self.up()
      corrupt(r)
      self.assert_dropped(c)
      c.close()
      c2 = self.reconnect()  # the server itself is fine
      self.assertEqual(c2.rpc(MAP_BAR, bar=0)[0], 0, corrupt.__name__)
      c2.close()

  def test_client_that_vanishes_mid_payload(self):
    c, r = self.up()
    r.put(struct.pack(REQ, MMIO_WRITE, 0, 0, 0, 1 << 20, 0) + bytes(1000))  # a megabyte promised, 1000 bytes sent
    c.close()
    c2 = self.reconnect()  # the server must have noticed and gone on to the next client
    self.assertEqual(c2.rpc(MAP_BAR, bar=0)[0], 0)

class TestRingStress(ServerCase):
  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    binary("ring_stress")

  def run_stress(self, iters:int, seed:int, caps:tuple[int, int]):
    self.start().close()  # the server serves one client at a time: let it go, the stress client takes its place
    dump = os.path.join(self.tmp, f"dump{seed}")
    r = subprocess.run([binary("ring_stress"), self.path, str(iters), str(seed), str(caps[0]), str(caps[1]), dump],
                       capture_output=True, text=True, timeout=150)
    self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
    c = Client(self.path)  # served after the posted writes of the stress client were
    self.addCleanup(c.close)
    c.rpc(MAP_BAR, bar=0)
    with open(dump, "rb") as f: expect = f.read()
    self.assertEqual(c.read(0, len(expect)), expect)  # what the client posted right before it closed is there
    return r.stdout

  def test_stress_default_rings(self): self.run_stress(12000, 1, (1 << 16, 1 << 16))
  def test_stress_tiny_rings(self): self.run_stress(6000, 2, (4096, 4096))
  def test_stress_uneven_rings(self): self.run_stress(12000, 3, (1 << 14, 1 << 15))

# ---- the Python client: the REAL TinyGPURing / APLRemotePCIDevice (extra/hcq1/remote.py) against the real server

def remote():
  if ROOT not in sys.path: sys.path.insert(0, ROOT)
  import extra.hcq1.remote as m
  return m

class Spy:
  """Stands in for the socket of a device and records the command of every request that goes out on it."""
  def __init__(self, s:socket.socket): self.s, self.cmds = s, []
  def sendall(self, data, *a):
    self.cmds.append(bytes(data)[0])
    return self.s.sendall(data, *a)
  def sendmsg(self, bufs, *a):
    self.cmds.append(bytes(bufs[0])[0])
    return self.s.sendmsg(bufs, *a)
  def __getattr__(self, name): return getattr(self.s, name)

class DeviceCase(ServerCase):
  def device(self, path:str|None=None, ring:bool=True, **caps:int):
    m = remote()
    env = {"APL_REMOTE_SOCK": path or self.path, "TINYGPU_RING": "1" if ring else "0"}
    m.getenv.cache_clear()  # getenv is cached: it would hand every device the environment of the first one
    try:
      with patch.dict(os.environ, env), patch.object(m.APLRemotePCIDevice, "ensure_app", classmethod(lambda cls: None)), \
           patch.object(m.System, "flock_acquire", return_value=-1), ring_caps(m, caps):  # no lock file, no app, rings of this size
        d = m.APLRemotePCIDevice("AM", "usb4")
    finally: m.getenv.cache_clear()
    self.addCleanup(d.close)
    return d

  def spy(self, d) -> Spy:
    d.sock = spy = Spy(d.sock)
    if d.ring is not None: d.ring.sock = spy
    return spy

class TestRingClient(DeviceCase):
  def ring(self, c:Client, **caps:int):
    m = remote()
    with ring_caps(m, caps): r = m.TinyGPURing.open(c.s, 0)
    self.assertIsNotNone(r)
    self.addCleanup(r.close)
    return r

  def test_open_and_roundtrip(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    r.write(0, 0x10, b"hello ring")
    self.assertEqual(r.read(0, 0x10, 10), b"hello ring")
    self.assertEqual(c.read(0x10, 10), b"hello ring")  # the other channel sees it
    r.write(0, 0x20, memoryview(bytearray(b"views work too")))
    self.assertEqual(r.read(0, 0x20, 14), b"views work too")

  def test_fragmented_setup_transfers_the_fd_once(self):
    class ShortSetup(Spy):
      def sendmsg(self, bufs, *args): return super().sendmsg([bufs[0][:5]], *args)
    c = self.start()
    c.s.settimeout(1)
    r = remote().TinyGPURing.open(ShortSetup(c.s), 0)
    self.addCleanup(r.close)
    c.rpc(MAP_BAR, bar=0)
    r.write(0, 16, b"fragment")
    self.assertEqual(r.read(0, 16, 8), b"fragment")

  def test_small_rings_wrap_and_carry_large_transfers(self):
    c = self.start()
    r = self.ring(c, REQ_CAP=4096, RSP_CAP=4096)
    c.rpc(MAP_BAR, bar=0)
    pattern = bytes((i * 31 + (i >> 8)) & 0xFF for i in range(1 << 20))
    r.write(0, 0, pattern)  # 256 times the ring
    self.assertEqual(r.read(0, 0, 1 << 20), pattern)
    rng, model = random.Random(11), bytearray(pattern[:1 << 16])
    for _ in range(600):
      off, n = rng.randrange(0, (len(model) - 1024) // 4) * 4, rng.randrange(1, 900)
      if rng.random() < 0.5:
        data = rng.randbytes(n)
        r.write(0, off, data)
        model[off:off + n] = data
      else: self.assertEqual(r.read(0, off, n), bytes(model[off:off + n]))

  def test_refused_read_keeps_the_ring(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    for off in (1 << 30, (1 << 64) - 8): self.assertRaises(RuntimeError, r.read, 0, off, 16)
    self.assertRaises(RuntimeError, r.read, 1, 0, 4)  # a BAR that was never mapped
    self.assertIsNone(r.error)
    r.write(0, 0, b"fine")
    self.assertEqual(r.read(0, 0, 4), b"fine")

  def test_sizes_the_server_would_not_take_are_refused_before_anything_is_sent(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    self.assertRaises(ValueError, r.write, 0, 0, bytes(BULK + 1))
    self.assertRaises(ValueError, r.read, 0, 0, BULK + 1)
    self.assertIsNone(r.error)
    r.write(0, 0, b"fine")
    self.assertEqual(r.read(0, 0, 4), b"fine")

  def test_socket_requests_between_ring_traffic(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    for i in range(200):
      data = struct.pack('<II', i, ~i & 0xFFFFFFFF)
      r.write(0, 8 * i, data)
      if i % 3 == 0: self.assertEqual(c.read(8 * i, 8), data)  # served after the ring entry before it
      if i % 5 == 0: self.assertEqual(c.rpc(CFG_READ, 0, 2)[:2], (0, 0x1002))
      if i % 7 == 0: self.assertEqual(r.read(0, 8 * i, 8), data)

  def test_kick_when_the_server_sleeps(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    for i in range(15):
      wait_until(lambda: r.words[r.SRV_STATE].value == 1, 10, "the server to go to sleep")
      r.write(0, 0x400, struct.pack('<I', i))
      self.assertGreaterEqual(r.kicks, i + 1)  # a publish that finds the server asleep wakes it, whatever else happens in between
      self.assertEqual(r.read(0, 0x400, 4), struct.pack('<I', i))

  def test_posted_writes_survive_eof(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    wait_until(lambda: r.words[r.SRV_STATE].value == 1, 10, "the server to go to sleep")
    r.write(0, 0x300, b"EOF!" * 100)
    r.close()
    c.close()
    c2 = self.reconnect()
    r2 = self.ring(c2)
    c2.rpc(MAP_BAR, bar=0)
    self.assertEqual(r2.read(0, 0x300, 400), b"EOF!" * 100)

  def test_closed_ring_raises(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    r.close()
    r.close()  # once
    self.assertRaises(RuntimeError, r.write, 0, 0, b"late")
    self.assertRaises(RuntimeError, r.read, 0, 0, 4)

  def test_server_death_is_an_error_not_a_hang(self):
    c = self.start()
    r = self.ring(c)
    c.rpc(MAP_BAR, bar=0)
    self.proc.kill()
    self.proc.wait()
    t0 = time.monotonic()
    self.assertRaises(ConnectionError, r.read, 0, 0, 4)
    self.assertLess(time.monotonic() - t0, 10)  # found out from the socket, not from the timeout
    self.assertIsInstance(r.error, ConnectionError)
    self.assertRaises(RuntimeError, r.write, 0, 0, b"x")  # the session is over

class TestAPLRingDevice(DeviceCase):
  def new(self, ring:bool=True, **caps:int):
    self.start().close()  # the server is up; the device is its next client
    return self.device(ring=ring, **caps)

  def test_default_is_the_socket(self):
    with patch.object(remote().TinyGPURing, "open", side_effect=AssertionError("the ring must be opt in")):
      d = self.new(ring=False)
    self.assertIsNone(d.ring)
    spy = self.spy(d)
    bar = d.map_bar(0)
    bar[0x10:0x18] = b"over the"
    self.assertEqual(bar[0x10:0x18], b"over the")
    self.assertEqual(spy.cmds, [MAP_BAR, MMIO_WRITE, MMIO_READ])  # no FEATURES, no ring: the wire of an old client

  def test_ring_carries_the_mmio(self):
    d = self.new()
    self.assertIsNotNone(d.ring)
    spy = self.spy(d)
    bar, bar32 = d.map_bar(0), d.map_bar(0, fmt='I')
    bar32[4] = 0xCAFEF00D
    self.assertEqual(bar32[4], 0xCAFEF00D)
    bar32[8:12] = [1, 2, 3, 4]
    self.assertEqual(bar32[8:12], [1, 2, 3, 4])
    self.assertEqual(bar[16:20], struct.pack('<I', 0xCAFEF00D))
    bar[0x100:0x108] = b"abcdefgh"
    self.assertEqual(bar[0x100:0x108], b"abcdefgh")
    self.assertNotIn(MMIO_READ, spy.cmds)
    self.assertNotIn(MMIO_WRITE, spy.cmds)
    self.assertIn(MAP_BAR, spy.cmds)  # the socket still does the rest

  def test_old_style_reads_see_ring_writes(self):
    d = self.new()
    remote_cls, bar = remote().RemotePCIDevice, d.map_bar(0)
    for i in range(300):
      data = struct.pack('<Q', i * 0x0101010101)
      bar[8 * i:8 * i + 8] = data
      if i % 4 == 0: self.assertEqual(remote_cls._bulk_read(d, 6, 0, 8 * i, 8), data)  # socket reads see earlier ring writes
      if i % 9 == 0: self.assertEqual(d.read_config(0, 2), 0x1002)
      if i % 6 == 0: d.write_config(0x10, i, 4)

  def test_small_rings(self):
    d = self.new(REQ_CAP=4096, RSP_CAP=4096)
    bar = d.map_bar(0)
    pattern = bytes((i * 7 + (i >> 9)) & 0xFF for i in range(1 << 20))
    bar[0:1 << 20] = pattern
    self.assertEqual(bar[0:1 << 20], pattern)

  def test_sleeping_server_is_woken(self):
    d = self.new()
    bar = d.map_bar(0)
    for i in range(10):
      wait_until(lambda: d.ring.words[d.ring.SRV_STATE].value == 1, 10, "the server to go to sleep")
      bar[0x40:0x44] = struct.pack('<I', i)
      self.assertEqual(bar[0x40:0x44], struct.pack('<I', i))
    self.assertGreaterEqual(d.ring.kicks, 10)

  def test_close_serves_what_was_posted(self):
    d = self.new()
    d.map_bar(0)[0x80:0x88] = b"posted!!"
    d.close()
    d2 = self.device()  # the server serves the ring of the first one before it takes the second
    self.assertEqual(d2.map_bar(0)[0x80:0x88], b"posted!!")

  def test_closed_device_raises(self):
    d = self.new()
    bar = d.map_bar(0)
    d.close()
    d.close()
    self.assertRaises(RuntimeError, lambda: bar[0:4])
    self.assertRaises(RuntimeError, lambda: bar.__setitem__(slice(0, 4), b"late"))

  def test_threads_share_one_session(self):
    d, errors = self.new(), []
    def work(k:int):
      try:
        bar = d.map_bar(0)
        for i in range(150):
          data = struct.pack('<II', k, i) * 4
          bar[k * 0x1000:k * 0x1000 + 32] = data
          assert bar[k * 0x1000:k * 0x1000 + 32] == data
          if i % 5 == 0: assert d.read_config(0, 2) == 0x1002
      except BaseException as e: errors.append(e)
    threads = [threading.Thread(target=work, args=(k,), daemon=True) for k in range(4)]
    for t in threads: t.start()
    for t in threads: t.join(60)
    self.assertFalse(any(t.is_alive() for t in threads))
    self.assertEqual(errors, [])

  def test_server_death_is_an_error_not_a_hang(self):
    d = self.new()
    bar = d.map_bar(0)
    self.proc.kill()
    self.proc.wait()
    t0 = time.monotonic()
    self.assertRaises(ConnectionError, lambda: bar[0:4])
    self.assertLess(time.monotonic() - t0, 10)
    self.assertRaises(RuntimeError, lambda: bar[0:4])  # no retry, no socket fallback

# ---- the server side of the wire, in Python: what the real server cannot be made to do on demand

class FakeServer:
  """A protocol peer for unsupported/refused rings and controlled setup or response failures."""
  def __init__(self, tmp:str, mode:str, error_text:bytes=b""):
    self.mode, self.error_text, self.kicks, self.cmds, self.eof, self.errors = mode, error_text, 0, [], False, []
    self.bar, self.conn, self.mm, self.words = bytearray(1 << 20), None, None, {}
    self.path = os.path.join(tmp, f"f{os.urandom(4).hex()}.sock")
    self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    self.listener.bind(self.path)
    self.listener.listen(1)
    threading.Thread(target=self.serve, daemon=True).start()

  def recvall(self, n:int) -> bytes:
    out = b""
    while len(out) < n:
      d = self.conn.recv(n - len(out))
      if not d: raise ConnectionError("closed")
      out += d
    return out

  def reply(self, status:int, r0:int, r1:int, data:bytes=b""): self.conn.sendall(struct.pack(RESP, status, r0, r1) + data)

  def serve(self):
    try:
      self.conn, _ = self.listener.accept()
      while True:
        msg, anc, _, _ = self.conn.recvmsg(33, socket.CMSG_LEN(4))
        fd = struct.unpack('i', anc[0][2][:4])[0] if anc else None
        if not msg: break
        if len(msg) < 33: msg += self.recvall(33 - len(msg))
        self.handle(*struct.unpack(REQ, msg), fd)
    except OSError: pass
    except BaseException as e: self.errors.append(e)
    self.eof = True

  def handle(self, cmd:int, dev:int, bar:int, a0:int, a1:int, a2:int, fd:int|None):
    self.cmds.append(cmd)
    if cmd == FEATURES and self.mode == "mute_features": return
    if cmd == RING_SETUP and self.mode in ("ring", "mute_setup", "bad_setup") and fd is not None: return self.setup(a0, fd)
    if fd is not None: os.close(fd)
    if cmd == CFG_READ: self.reply(0, 0x1002, 0)
    elif cmd == MAP_BAR: self.reply(0, 0, len(self.bar))
    elif cmd == MMIO_READ: self.reply(0, a1, 0, bytes(self.bar[a0:a0 + a1]))
    elif cmd == MMIO_WRITE: self.bar[a0:a0 + a1] = self.recvall(a1)
    elif cmd == FEATURES and self.error_text: self.reply(1, len(self.error_text), 0, self.error_text)
    elif cmd == FEATURES and self.mode != "old": self.reply(0, 0, PROTO_VERSION << 32 | FEAT_FEATURES | FEAT_RING)
    elif cmd == RING_SETUP and self.mode == "refuse": self.reply(1, KR_BAD_ARG, 1)
    elif cmd == RING_KICK and self.mode == "ring": self.kicks += 1
    else: self.reply(1, 0, 0)

  def setup(self, nbytes:int, fd:int):
    self.mm = mmap.mmap(fd, nbytes)
    os.close(fd)
    magic, version, self.req_cap, self.rsp_cap, data_off = struct.unpack_from('<QIIII', self.mm)
    assert (magic, version, data_off) == (RING_MAGIC, 1, HDR)
    self.words = {off: ctypes.c_uint64.from_buffer(self.mm, off)
                  for off in (RING_REQ_HEAD, RING_REQ_TAIL, RING_RSP_HEAD, RING_RSP_TAIL, RING_SRV_STATE)}
    self.tail = self.head = 0
    if self.mode != "mute_setup": self.reply(0, nbytes - 1 if self.mode == "bad_setup" else nbytes, 1)

  def set_word(self, off:int, val:int): self.words[off].value = val

  def request_bytes(self, pos:int, n:int) -> bytes:
    p = pos & (self.req_cap - 1)
    first = min(n, self.req_cap - p)
    return bytes(self.mm[HDR + p:HDR + p + first]) + bytes(self.mm[HDR:HDR + n - first])

  def wait_requests(self, n:int=1, timeout:float=10.0) -> list[tuple[tuple, bytes]]:
    """The next n requests on the request ring, as the server takes them: the header and, for a write, its payload."""
    out, end = [], time.monotonic() + timeout
    while len(out) < n:
      avail = self.words[RING_REQ_HEAD].value - self.tail
      if avail >= 33:
        hdr = struct.unpack(REQ, self.request_bytes(self.tail, 33))
        need = 33 + (hdr[4] if hdr[0] == MMIO_WRITE else 0)
        if avail >= need:
          out.append((hdr, self.request_bytes(self.tail + 33, need - 33)))
          self.tail += need
          self.words[RING_REQ_TAIL].value = self.tail
          continue
      if time.monotonic() > end: raise TimeoutError("no request on the ring")
      time.sleep(0.001)
    return out

  def answer(self, status:int, r0:int, r1:int, data:bytes=b""):
    msg, base = struct.pack(RESP, status, r0, r1) + data, HDR + self.req_cap
    p = self.head & (self.rsp_cap - 1)
    first = min(len(msg), self.rsp_cap - p)
    self.mm[base + p:base + p + first] = msg[:first]
    self.mm[base:base + len(msg) - first] = msg[first:]
    self.head += len(msg)
    self.words[RING_RSP_HEAD].value = self.head

  def answer_later(self, status:int, r0:int, r1:int, data:bytes=b""):  # when the next request has arrived
    def run():
      with contextlib.suppress(TimeoutError):
        self.wait_requests(1)
        self.answer(status, r0, r1, data)
    threading.Thread(target=run, daemon=True).start()

  def drop(self):
    with contextlib.suppress(OSError): self.conn.shutdown(socket.SHUT_RDWR)
    self.conn.close()

class TestRingClientAgainstFakeServers(DeviceCase):
  @classmethod
  def setUpClass(cls):
    if sys.platform != "darwin": raise unittest.SkipTest("the ring client needs libSystem")
    cls.tmp = tempfile.mkdtemp(prefix="tgringfake")

  def fake(self, mode:str, **kw) -> FakeServer:
    s = FakeServer(self.tmp, mode, **kw)
    self.addCleanup(lambda: (s.errors and self.fail(f"the fake server failed: {s.errors[0]!r}"), s.drop() if s.conn else None))
    return s

  def test_old_server_keeps_the_socket(self):
    s = self.fake("old")
    d = self.device(s.path)
    self.assertIsNone(d.ring)  # FEATURES answered (1, 0, 0): nothing was set up, nothing to clean up
    bar = d.map_bar(0)
    bar[0x10:0x14] = b"abcd"
    self.assertEqual(bar[0x10:0x14], b"abcd")
    self.assertEqual(bytes(s.bar[0x10:0x14]), b"abcd")
    self.assertEqual(s.cmds, [FEATURES, MAP_BAR, MMIO_WRITE, MMIO_READ])

  def test_refused_ring_keeps_the_socket(self):
    s = self.fake("refuse")
    d = self.device(s.path)
    self.assertIsNone(d.ring)
    bar = d.map_bar(0)
    bar[0x10:0x14] = b"abcd"
    self.assertEqual(bar[0x10:0x14], b"abcd")
    self.assertEqual(s.cmds, [FEATURES, RING_SETUP, MAP_BAR, MMIO_WRITE, MMIO_READ])

  def test_error_text_of_the_server_is_raised(self):
    s = self.fake("ring", error_text=b"Driver not available.")
    with self.assertRaisesRegex(RuntimeError, "Driver not available"): self.device(s.path)

  def test_negotiation_timeout_closes_the_connection(self):
    for mode in ("mute_features", "mute_setup"):
      with self.subTest(mode=mode):
        s = self.fake(mode)
        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(conn.close)
        conn.connect(s.path)
        conn.settimeout(2)
        started = time.monotonic()
        with patch.object(remote().TinyGPURing, "TIMEOUT", 0.05):
          self.assertRaises(TimeoutError, remote().TinyGPURing.open, conn, 0)
        self.assertLess(time.monotonic() - started, 1)
        wait_until(lambda: s.eof, 2, "the server to see failed negotiation EOF")

  def test_malformed_setup_acknowledgement_closes_the_connection(self):
    s = self.fake("bad_setup")
    with self.assertRaisesRegex(RuntimeError, "set up a ring"): self.device(s.path)
    wait_until(lambda: s.eof, 2, "the server to see failed setup EOF")

  def test_without_the_opt_in_nothing_is_negotiated(self):
    s = self.fake("ring")
    d = self.device(s.path, ring=False)
    self.assertIsNone(d.ring)
    d.map_bar(0)[0:4] = b"abcd"
    self.assertEqual(d.map_bar(0)[0:4], b"abcd")
    self.assertEqual(s.cmds, [MAP_BAR, MMIO_WRITE, MMIO_READ])  # the wire of an old client

  def test_kick_only_when_asleep_and_once_per_sleep(self):
    s = self.fake("ring")
    d = self.device(s.path)
    ring, state = d.ring, lambda: s.words[RING_SRV_STATE].value
    self.assertIsNotNone(ring)
    def post(i:int): ring.write(0, 4 * i, struct.pack('<I', i))
    post(0)  # srv_state is 0: awake, nothing to wake
    self.assertEqual(ring.kicks, 0)
    s.set_word(RING_SRV_STATE, 1)  # the server announces its sleep
    post(1)
    self.assertEqual((ring.kicks, state()), (1, 2))  # one compare and swap 1 -> 2 and one kick
    post(2)
    post(3)
    self.assertEqual((ring.kicks, state()), (1, 2))  # already woken: nothing more to send
    s.set_word(RING_SRV_STATE, 0)
    post(4)
    self.assertEqual((ring.kicks, state()), (1, 0))
    s.set_word(RING_SRV_STATE, 1)
    post(5)
    self.assertEqual((ring.kicks, state()), (2, 2))
    wait_until(lambda: s.kicks == 2, 10, "the kicks to arrive")
    reqs = s.wait_requests(6)
    self.assertEqual([r[1] for r in reqs], [struct.pack('<I', i) for i in range(6)])
    self.assertTrue(all(r[0][:5] == (MMIO_WRITE, 0, 0, 4 * i, 4) for i, r in enumerate(reqs)))  # in order, nothing lost between the kicks
    self.assertEqual(s.cmds.count(RING_KICK), 2)

  def test_mute_server_times_out_and_ends_the_session(self):
    s = self.fake("ring")
    d = self.device(s.path)
    bar, d.ring.TIMEOUT = d.map_bar(0), 0.3
    t0 = time.monotonic()
    self.assertRaises(TimeoutError, lambda: bar[0:4])
    self.assertLess(time.monotonic() - t0, 10)
    self.assertIsInstance(d.ring.error, TimeoutError)
    self.assertIsNone(d.ring.mm)  # the mapping is gone
    self.assertRaises(RuntimeError, lambda: bar[0:4])  # no retry, and nothing goes to the socket instead
    self.assertRaises((OSError, RuntimeError), d.read_config, 0, 2)  # the socket is shut down
    wait_until(lambda: s.eof, 10, "the server to see EOF")
    self.assertEqual(s.cmds.count(MMIO_READ), 0)

  def test_server_that_goes_away_while_the_client_waits(self):
    s = self.fake("ring")
    d = self.device(s.path)
    bar = d.map_bar(0)
    timer = threading.Timer(0.2, s.drop)
    self.addCleanup(timer.cancel)
    timer.start()
    t0 = time.monotonic()
    self.assertRaises(ConnectionError, lambda: bar[0:4])
    self.assertLess(time.monotonic() - t0, 10)

  def test_response_head_that_makes_no_sense(self):
    s = self.fake("ring")
    d = self.device(s.path)
    bar = d.map_bar(0)
    timer = threading.Timer(0.1, s.set_word, (RING_RSP_HEAD, 1 << 30))  # more than a ring ahead of the tail
    self.addCleanup(timer.cancel)
    timer.start()
    self.assertRaisesRegex(RuntimeError, "response head", lambda: bar[0:4])
    self.assertIsNone(d.ring.mm)

  def test_request_tail_that_makes_no_sense(self):
    s = self.fake("ring")
    d = self.device(s.path)
    bar = d.map_bar(0)
    s.set_word(RING_REQ_TAIL, 1 << 40)  # ahead of the head: nobody could have consumed that
    self.assertRaisesRegex(RuntimeError, "request tail", lambda: bar.__setitem__(slice(0, 4), b"abcd"))
    self.assertRaises(RuntimeError, lambda: bar[0:4])

  def test_answer_of_the_wrong_size_is_a_protocol_error(self):
    s = self.fake("ring")
    d = self.device(s.path)
    bar = d.map_bar(0)
    s.answer_later(0, 3, 0, b"abc")  # three bytes for a read of four
    self.assertRaisesRegex(RuntimeError, "3 bytes to a read of 4", lambda: bar[0:4])
    self.assertIsNotNone(d.ring.error)

  def test_refused_read_is_not_the_end_of_the_session(self):
    s = self.fake("ring")
    d = self.device(s.path)
    bar = d.map_bar(0)
    s.answer_later(1, 0, 0)
    self.assertRaisesRegex(RuntimeError, "refused", lambda: bar[0:4])
    self.assertIsNone(d.ring.error)
    s.answer_later(0, 4, 0, b"abcd")
    self.assertEqual(bar[0:4], b"abcd")

if __name__ == "__main__": unittest.main()
