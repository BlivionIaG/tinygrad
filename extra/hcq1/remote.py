from __future__ import annotations
import os, mmap, array, functools, contextlib, itertools, struct, socket, subprocess, time, enum, atexit, ctypes, threading, select
from tinygrad.helpers import getenv, temp, ceildiv, unwrap, fetch, system, _ensure_downloads_dir, DEBUG, flatten, OSX
from tinygrad.runtime.support.memory import MMIOInterface
from tinygrad.runtime.support.system import FileIOInterface
from tinygrad.runtime.support.system import PCIDevice, System

class RemoteCmd(enum.IntEnum):
  PROBE,MAP_BAR,MAP_SYSMEM_FD,CFG_READ,CFG_WRITE,RESET,MMIO_READ,MMIO_WRITE,MAP_SYSMEM,SYSMEM_READ,SYSMEM_WRITE,RESIZE_BAR,PING = range(13)

class RemoteMMIOInterface(MMIOInterface):
  def __init__(self, dev:RemotePCIDevice, residx:int, nbytes:int, fmt='B', off=0, rd_cmd=RemoteCmd.MMIO_READ, wr_cmd=RemoteCmd.MMIO_WRITE):
    self.dev, self.residx, self.nbytes, self.fmt, self.off, self.el_sz = dev, residx, nbytes, fmt, off, struct.calcsize(fmt)
    self.rd_cmd, self.wr_cmd = rd_cmd, wr_cmd

  def __getitem__(self, index):
    sl = index if isinstance(index, slice) else slice(index, index + 1)
    start, stop = (sl.start or 0) * self.el_sz, (sl.stop or len(self)) * self.el_sz
    data = self.dev._bulk_read(self.rd_cmd, self.residx, self.off + start, stop - start)
    result = data if self.fmt == 'B' else list(struct.unpack(f'<{(stop - start) // self.el_sz}{self.fmt}', data))
    return result if isinstance(index, slice) else result[0]

  def __setitem__(self, index, val):
    start = (index.start or 0) * self.el_sz if isinstance(index, slice) else index * self.el_sz
    data = (val if self.fmt == 'B' else struct.pack(f'<{len(val)}{self.fmt}', *val)) if isinstance(index, slice) else struct.pack(f'<{self.fmt}', val)
    self.dev._bulk_write(self.wr_cmd, self.residx, self.off + start, data)

  def view(self, offset:int=0, size:int|None=None, fmt=None):
    return RemoteMMIOInterface(self.dev, self.residx, size or (self.nbytes - offset), fmt or self.fmt, self.off + offset, self.rd_cmd, self.wr_cmd)

class RemotePCIDevice(PCIDevice):
  _bulk_sent:int = 0
  _bulk_recv:int = 0
  _rpc_count:int = 0
  _start_time:float = 0.0

  @staticmethod
  @functools.cache
  def remote_sock(host:str, port:int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(getenv("REMOTE_TIMEOUT", 3))
    sock.connect((host, port))
    sock.settimeout(None)
    if DEBUG >= 1 and RemotePCIDevice._start_time == 0.0:
      RemotePCIDevice._start_time = time.perf_counter()
      def _print_stats():
        dt = time.perf_counter() - RemotePCIDevice._start_time
        sent_mb, recv_mb = RemotePCIDevice._bulk_sent / 1e6, RemotePCIDevice._bulk_recv / 1e6
        print(f"remote: sent {sent_mb:,.2f} MB ({sent_mb/dt:,.2f} MB/s), recv {recv_mb:,.2f} MB ({recv_mb/dt:,.2f} MB/s), "
              f"{RemotePCIDevice._rpc_count:,} roundtrips in {dt:.2f}s")
      atexit.register(_print_stats)
    return sock

  @staticmethod
  @functools.cache
  def remote_list(vendor:int, devices:tuple[tuple[int, tuple[int, ...]], ...], base_class:int|None) -> list[tuple[socket.socket, str]]:
    payload = array.array('I', itertools.chain.from_iterable((m, d) for m, ds in devices for d in ds)).tobytes()
    def q(r:str) -> list[tuple[socket.socket, str]]:
      sock = RemotePCIDevice.remote_sock((host:=r.strip().split(":")[0]), (port:=int(r.strip().split(":")[1]) if ":" in r else 6667))
      data_len, _, _, _ = RemotePCIDevice._rpc(sock, 0, RemoteCmd.PROBE, base_class or 0, len(payload), vendor, payload=payload)
      return [(sock, f"remote:{host}:{port}:{d}") for d in RemotePCIDevice._recvall(sock, data_len).decode().split('\n')]
    return flatten([q(r) for r in getenv("REMOTE", "").split(",") if r.strip()])

  @staticmethod
  def _recvall(sock:socket.socket, n:int) -> bytes:
    data = b''
    while len(data) < n and (chunk:=sock.recv(n - len(data))): data += chunk
    if len(data) < n: raise RuntimeError("Connection closed")
    return data

  @staticmethod
  def _rpc(sock:socket.socket, dev_id:int, cmd:int, *args:int, bar:int=0, readout_size:int=0, payload:bytes=b'', has_fd=False):
    sock.sendall(struct.pack('<BIIQQQ', cmd, dev_id, bar, *(*args, 0, 0, 0)[:3]) + payload)
    if has_fd:
      msg, anc, _, _ = sock.recvmsg(17, socket.CMSG_LEN(4))
      fd = struct.unpack('<i', anc[0][2][:4])[0]
    else: msg, fd = RemotePCIDevice._recvall(sock, 17), None
    if (resp:=struct.unpack('<BQQ', msg))[0] != 0:
      raise RuntimeError(f"RPC failed: {RemotePCIDevice._recvall(sock, resp[1]).decode('utf-8') if resp[1] > 0 else 'unknown error'}")
    RemotePCIDevice._rpc_count += 1
    return (resp[1], resp[2]) + ((RemotePCIDevice._recvall(sock, readout_size) if readout_size > 0 else None),) + (fd,)

  def __init__(self, devpref:str, pcibus:str, sock:socket.socket):
    self.sock, self.pcibus, self.dev_id = sock, pcibus, int(pcibus.split(':')[-1]) if ':' in pcibus else 0
    self.peer_group = sock.getpeername()[0]
    for buft in [socket.SO_SNDBUF, socket.SO_RCVBUF]: self.sock.setsockopt(socket.SOL_SOCKET, buft, 64 << 20)

    self.lock_fd = System.flock_acquire(f"{devpref.lower()}_{pcibus.lower()}.lock")

  def rpc(self, cmd:int, *args:int, **kwargs):
    return self._rpc(self.sock, self.dev_id, cmd, *args, **kwargs)

  def _bulk_read(self, cmd:int, idx:int, offset:int, size:int) -> bytes:
    RemotePCIDevice._bulk_recv += size
    return unwrap(self.rpc(cmd, offset, size, bar=idx, readout_size=size)[2])
  def _bulk_write(self, cmd:int, idx:int, offset:int, data:bytes):
    RemotePCIDevice._bulk_sent += len(data)
    self.sock.sendall(struct.pack('<BIIQQQ', cmd, self.dev_id, idx, offset, len(data), 0) + data)

  def alloc_sysmem(self, size:int, vaddr:int=0, contiguous:bool=False) -> tuple[MMIOInterface, list[int]]:
    paddrs_len, handle, _, _ = self.rpc(RemoteCmd.MAP_SYSMEM, size, int(contiguous))
    paddrs = list(struct.unpack(f'<{paddrs_len // 8}Q', self._recvall(self.sock, paddrs_len)))
    return RemoteMMIOInterface(self, handle, size, fmt='B', rd_cmd=RemoteCmd.SYSMEM_READ, wr_cmd=RemoteCmd.SYSMEM_WRITE), paddrs

  def reset(self): self.rpc(RemoteCmd.RESET)
  def read_config(self, offset:int, size:int): return self.rpc(RemoteCmd.CFG_READ, offset, size)[0]
  def write_config(self, offset:int, value:int, size:int): self.rpc(RemoteCmd.CFG_WRITE, offset, size, value)

  @functools.cache
  def bar_info(self, bar_idx:int) -> tuple[int, int]: return self.rpc(RemoteCmd.MAP_BAR, bar=bar_idx)[:2]
  def map_bar(self, bar:int, off:int=0, addr:int=0, size:int|None=None, fmt='B') -> MMIOInterface:
    return RemoteMMIOInterface(self, bar, size or self.bar_info(bar)[1], fmt).view(off, size, fmt)
  def resize_bar(self, bar_idx:int): self.rpc(RemoteCmd.RESIZE_BAR, bar=bar_idx)

class TinyGPURing:
  """Opt-in TinyGPU MMIO transport; the protocol is described in installer/Shared/server.c.

  The owner serializes ring and socket operations as one stream. A failure after possible acceptance ends the session:
  close the mapping and shut down the socket. Never replay over the socket: a posted write could land twice."""
  FEATURES, SETUP, KICK = 18, 19, 20  # the commands of the TinyGPU server that negotiate and run the ring
  FEAT_RING, PROTO_VERSION = 1 << 4, 1
  MAGIC, HDR = 0x5447524E47000001, 16384
  REQ_HEAD, REQ_TAIL, RSP_HEAD, RSP_TAIL, SRV_STATE = 128, 256, 384, 512, 640  # the shared words, in the header
  REQ_CAP, RSP_CAP = 1 << 20, 1 << 20  # bytes of the request and of the response ring: powers of two, at least 4096
  MAX_BULK = 64 << 20  # the server's bulk buffer: a bigger read is refused, a bigger write would make it drop us
  TIMEOUT = 20.0  # seconds without any progress after which a wait for the server gives up
  REQ, RESP = '<BIIQQQ', '<BQQ'

  @staticmethod
  def _cas64():
    # an atomic compare and swap with a full barrier around it, from libSystem: looked up here and not at import, the ring is Darwin only
    cas = System.libsys.OSAtomicCompareAndSwap64Barrier
    cas.argtypes, cas.restype = [ctypes.c_int64, ctypes.c_int64, ctypes.c_void_p], ctypes.c_bool
    return cas

  @staticmethod
  def _call(sock:socket.socket, dev_id:int, cmd:int, *args:int, fd:int|None=None) -> tuple[int, int, int]:
    pkt = struct.pack(TinyGPURing.REQ, cmd, dev_id, 0, *(*args, 0, 0, 0)[:3])
    if fd is None: sock.sendall(pkt)
    else:
      sent = sock.sendmsg([pkt], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('i', fd))])
      if sent == 0: raise ConnectionError("TinyGPU ring setup sent no bytes")
      if sent < len(pkt): sock.sendall(pkt[sent:])
    return struct.unpack(TinyGPURing.RESP, RemotePCIDevice._recvall(sock, struct.calcsize(TinyGPURing.RESP)))

  @staticmethod
  def _shm(nbytes:int) -> tuple[int, mmap.mmap]:
    import _posixshmem  # the object only has to live until the server has its descriptor: the name goes away at once
    name = f"/tgr{os.getpid()}_{os.urandom(4).hex()}"  # macOS: 31 characters at most
    fd = _posixshmem.shm_open(name, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
      _posixshmem.shm_unlink(name)
      os.ftruncate(fd, nbytes)
      return fd, mmap.mmap(fd, nbytes)
    except BaseException:
      os.close(fd)
      raise

  @classmethod
  def open(cls, sock:socket.socket, dev_id:int) -> TinyGPURing|None:
    """Return None for unsupported or refused rings; otherwise establish the transport or fail the connection."""
    if not OSX: return None
    try:
      cas = cls._cas64()
      System.memory_barrier()
    except (AttributeError, OSError): return None  # no libSystem helpers to order memory with
    timeout = sock.gettimeout()
    try:
      sock.settimeout(cls.TIMEOUT if timeout is None else min(timeout, cls.TIMEOUT))
      status, resp0, resp1 = cls._call(sock, dev_id, cls.FEATURES, cls.PROTO_VERSION)
      if status and resp0: raise RuntimeError(f"RPC failed: {RemotePCIDevice._recvall(sock, resp0).decode('utf-8')}")
      if status or (resp1 >> 32) < cls.PROTO_VERSION or not resp1 & cls.FEAT_RING:
        if DEBUG >= 1: print("TinyGPU server has no command ring, MMIO stays on the socket")
        return None
      req_cap, rsp_cap = cls.REQ_CAP, cls.RSP_CAP
      nbytes = cls.HDR + req_cap + rsp_cap
      fd, mm = cls._shm(nbytes)
      try:
        struct.pack_into('<QIIII', mm, 0, cls.MAGIC, 1, req_cap, rsp_cap, cls.HDR)
        status, resp0, _ = cls._call(sock, dev_id, cls.SETUP, nbytes, fd=fd)
      except BaseException:
        mm.close()
        raise
      finally: os.close(fd)
      if status:
        mm.close()
        if DEBUG >= 1: print(f"TinyGPU server refused the command ring ({resp0:#x}), MMIO stays on the socket")
        return None
      if resp0 != nbytes:
        mm.close()
        raise RuntimeError(f"TinyGPU server set up a ring of {resp0} bytes, asked for {nbytes}")
      return cls(sock, dev_id, mm, cas, req_cap, rsp_cap)
    except BaseException:
      with contextlib.suppress(OSError): sock.shutdown(socket.SHUT_RDWR)
      sock.close()
      raise
    finally:
      if sock.fileno() >= 0: sock.settimeout(timeout)

  def __init__(self, sock:socket.socket, dev_id:int, mm:mmap.mmap, cas, req_cap:int, rsp_cap:int):
    self.sock, self.dev_id, self.cas, self.req_cap, self.rsp_cap = sock, dev_id, cas, req_cap, rsp_cap
    self.mm:mmap.mmap|None = mm
    # every shared word is one aligned 64-bit access (struct.pack_into would store it byte by byte: the server could see a torn counter)
    self.words = {off: ctypes.c_uint64.from_buffer(mm, off) for off in (self.REQ_HEAD, self.REQ_TAIL, self.RSP_HEAD, self.RSP_TAIL, self.SRV_STATE)}
    self.head, self.tail = 0, 0  # our request head and response tail: the words only we write
    self.kicks = 0
    self.error:BaseException|None = None

  # Python's own loads and stores are not ordered against another process (the GIL orders nothing between processes): an aligned 64-bit access is one
  # access, and a full fence after a load makes it an acquire, one before a store makes it a release.
  def _load(self, off:int) -> int:
    val = self.words[off].value
    System.memory_barrier()
    return val
  def _store(self, off:int, val:int):
    System.memory_barrier()
    self.words[off].value = val

  def close(self):
    """Gives our mapping back, once. The server keeps its own: what is in the request ring when the socket closes is still served."""
    if self.mm is None: return
    self.words = {}  # the views pin the mapping
    mm, self.mm = self.mm, None
    with contextlib.suppress(BufferError): mm.close()  # a traceback may still hold a view: the mapping then goes with the last of them

  def abort(self, error:BaseException):
    self.error = self.error or error
    self.close()
    with contextlib.suppress(OSError): self.sock.shutdown(socket.SHUT_RDWR)  # the server sees EOF, serves the ring, frees everything

  @contextlib.contextmanager
  def _op(self):
    if self.mm is None: raise RuntimeError("TinyGPU command ring is closed" + (f" after: {self.error!r}" if self.error is not None else ""))
    try: yield
    except BaseException as e:
      self.abort(e)
      raise

  def _stall(self, spins:int, since:float) -> int:
    # nothing moved yet: look at the clock and at the socket now and then, sleep a little once it takes long
    if spins > 4096: time.sleep(50e-6)
    if (spins & 255) == 255:
      if time.monotonic() - since > self.TIMEOUT: raise TimeoutError(f"TinyGPU server moved nothing in its ring for {self.TIMEOUT:g} s")
      try: gone = bool(select.select([self.sock], [], [], 0)[0]) and not self.sock.recv(1, socket.MSG_PEEK)
      except (OSError, ValueError) as e: raise ConnectionError(f"TinyGPU server connection failed: {e}") from e
      if gone: raise ConnectionError("TinyGPU server closed the connection")
    return spins + 1

  def _publish(self, head:int):
    self.head = head
    self._store(self.REQ_HEAD, head)  # the bytes before the head
    System.memory_barrier()  # the head before the look at srv_state: our half of the Dekker handshake with the server's sleep
    # Only the first producer that sees the announced sleep sends a wake-up.
    if self.words[self.SRV_STATE].value == 1 and self.cas(1, 2, ctypes.addressof(self.words[self.SRV_STATE])):
      self.kicks += 1
      self.sock.sendall(struct.pack(self.REQ, self.KICK, self.dev_id, 0, 0, 0, 0))

  def _put(self, *parts:bytes|memoryview):
    # the pieces go into the request ring one after the other and are published together; what does not fit goes in as the server frees room
    h, cap, spins, since = self.head, self.req_cap, 0, time.monotonic()
    mm = unwrap(self.mm)
    for part in parts:
      mv = memoryview(part).cast('B')
      while len(mv):
        used = h - self._load(self.REQ_TAIL)
        if not 0 <= used <= cap: raise RuntimeError(f"TinyGPU server moved the request tail to {h - used}, the head is at {h}")
        if used == cap:  # full: show what is there so the server can drain it, and wait
          if h != self.head: self._publish(h)
          spins = self._stall(spins, since)
          continue
        n = min(cap - used, len(mv))
        pos = h & (cap - 1)
        first = min(n, cap - pos)
        mm[self.HDR + pos:self.HDR + pos + first] = mv[:first]
        if n > first: mm[self.HDR:self.HDR + n - first] = mv[first:n]
        h, mv, spins, since = h + n, mv[n:], 0, time.monotonic()
    self._publish(h)

  def _get(self, n:int) -> bytes:
    # n bytes of the response ring, as the server produces them
    parts, cap, base, spins, since = [], self.rsp_cap, self.HDR + self.req_cap, 0, time.monotonic()
    mm = unwrap(self.mm)
    while n:
      avail = self._load(self.RSP_HEAD) - self.tail
      if not 0 <= avail <= cap: raise RuntimeError(f"TinyGPU server moved the response head to {self.tail + avail}, the tail is at {self.tail}")
      if not avail:
        spins = self._stall(spins, since)
        continue
      k = min(avail, n)
      pos = self.tail & (cap - 1)
      first = min(k, cap - pos)
      parts.append(mm[base + pos:base + pos + first])
      if k > first: parts.append(mm[base:base + k - first])
      self.tail += k
      self._store(self.RSP_TAIL, self.tail)  # after the bytes were read: the room is the server's again
      n, spins, since = n - k, 0, time.monotonic()
    return parts[0] if len(parts) == 1 else b''.join(parts)

  def read(self, bar:int, off:int, size:int) -> bytes:
    if size > self.MAX_BULK: raise ValueError(f"TinyGPU ring: a read of {size} bytes is more than the server's {self.MAX_BULK}")
    with self._op():
      self._put(struct.pack(self.REQ, RemoteCmd.MMIO_READ, self.dev_id, bar, off, size, 0))
      status, resp0, _ = struct.unpack(self.RESP, self._get(struct.calcsize(self.RESP)))
      if status == 0 and resp0 != size: raise RuntimeError(f"TinyGPU server answered {resp0} bytes to a read of {size}")
      data = self._get(size) if status == 0 else b''
    if status: raise RuntimeError(f"TinyGPU server refused to read {size} bytes at {off:#x} of BAR {bar}")  # the ring itself is fine
    return data

  def write(self, bar:int, off:int, data:bytes|memoryview):
    """Posted, like on the socket: nothing comes back, the server drops a write outside the BAR."""
    mv = memoryview(data).cast('B')
    if len(mv) > self.MAX_BULK: raise ValueError(f"TinyGPU ring: a write of {len(mv)} bytes is more than the server's {self.MAX_BULK}")
    with self._op(): self._put(struct.pack(self.REQ, RemoteCmd.MMIO_WRITE, self.dev_id, bar, off, len(mv), 0), mv)

class APLRemotePCIDevice(RemotePCIDevice):
  APP_PATH = "/Applications/TinyGPU.app/Contents/MacOS/TinyGPU"

  @classmethod
  def ensure_app(cls):
    commit = "c0d024f9ff0e1dc8fdf217f255da7101d91e8323"
    app_name = f"TinyGPU_{commit}.zip"
    if (_ensure_downloads_dir() / app_name).is_file() and os.path.exists(cls.APP_PATH): return
    print("Downloading TinyGPU.app...")
    with contextlib.suppress(RuntimeError): system("pkill -f TinyGPU")
    system(f"ditto -xk {fetch(f'https://github.com/tinygrad/tinygpu_releases/raw/{commit}/TinyGPU.zip', name=app_name)} /Applications")
    print(system(f"{cls.APP_PATH} install"))

  def __init__(self, devpref:str, pcibus:str):
    self.ensure_app()
    sock_path, sock = getenv("APL_REMOTE_SOCK", temp("tinygpu.sock")), socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    for i in range(100):
      with contextlib.suppress(ConnectionRefusedError, FileNotFoundError):
        sock.connect(sock_path)
        break
      if i == 0: subprocess.Popen([self.APP_PATH, "server", sock_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
      time.sleep(0.05)
    else: raise RuntimeError(f"Failed to connect to TinyGPU server at {sock_path}.")
    super().__init__(devpref, "usb4", sock=sock)
    self.lock = threading.RLock()  # the socket and the ring are one request stream: one operation at a time
    self.ring = TinyGPURing.open(sock, self.dev_id) if getenv("TINYGPU_RING", 0) else None  # opt in; None: MMIO over the socket, as always

  # Socket controls are served after ring entries published before them.
  def rpc(self, cmd:int, *args:int, **kwargs):
    with self.lock: return super().rpc(cmd, *args, **kwargs)
  def _bulk_read(self, cmd:int, idx:int, offset:int, size:int) -> bytes:
    with self.lock:
      if self.ring is None or cmd != RemoteCmd.MMIO_READ: return super()._bulk_read(cmd, idx, offset, size)
      RemotePCIDevice._bulk_recv += size
      return self.ring.read(idx, offset, size)
  def _bulk_write(self, cmd:int, idx:int, offset:int, data:bytes):
    with self.lock:
      if self.ring is None or cmd != RemoteCmd.MMIO_WRITE: return super()._bulk_write(cmd, idx, offset, data)
      RemotePCIDevice._bulk_sent += len(data)
      self.ring.write(idx, offset, data)

  def close(self):
    """Ends the session: what is in the ring is still served, then the server frees everything."""
    with self.lock:
      if self.ring is not None: self.ring.close()
      self.sock.close()

  def alloc_sysmem(self, size:int, vaddr:int=0, contiguous:bool=False) -> tuple[MMIOInterface, list[int]]:
    mapped_size, _, _, fd = self.rpc(RemoteCmd.MAP_SYSMEM_FD, size, int(contiguous), has_fd=True)
    memview = MMIOInterface(FileIOInterface(fd=fd).mmap(0, mapped_size, mmap.PROT_READ | mmap.PROT_WRITE, mmap.MAP_SHARED, 0), mapped_size, fmt='B')

    # paddrs are returned as (paddr, size) pairs until a (paddr=0, size=0) terminator in the beginning of the mapping.
    paddrs_raw = list(itertools.takewhile(lambda p: p[1] != 0, zip(memview.view(fmt='Q')[0::2], memview.view(fmt='Q')[1::2])))
    return memview, [p + i for p, sz in paddrs_raw for i in range(0, sz, 0x1000)][:ceildiv(size, 0x1000)]
