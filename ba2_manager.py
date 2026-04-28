"""
BA2 Manager — Standalone BA2 Archive Tool
Combines BSA Browser + BAMgr functionality:
  • Open and browse BA2 archives (General + DX10/Texture)
  • Extract single files, folders, or entire archives
  • Create new BA2 archives (General or DX10) with full settings
  • Add files to an existing archive
  • Replace files inside an existing archive
  • Delete files from an archive
  • Preview file info (texture dimensions, format, compression)

Supports Fallout 4 / Fallout 76 (BA2 v1, v7, v8) and Starfield (BA2 v2–v3).
Requires: Python 3.8+, PyQt5
"""

import sys
import os
import struct
import zlib
import threading
from pathlib import Path
from typing import List, Optional, Dict

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
    QSplitter, QToolBar, QAction, QStatusBar, QLabel, QFileDialog,
    QMessageBox, QDialog, QDialogButtonBox, QFormLayout, QComboBox,
    QCheckBox, QGroupBox, QProgressDialog, QLineEdit,
    QPushButton, QTabWidget, QHeaderView, QAbstractItemView, QMenu,
    QProgressBar, QTextEdit, QListWidget, QListWidgetItem,
    QRadioButton, QToolButton
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt5.QtGui import QColor, QFont, QPalette

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

MAGIC                = b"BTDX"
ARCHIVE_TYPE_GENERAL = b"GNRL"
ARCHIVE_TYPE_DX10    = b"DX10"
# Header "version": 1 = FO4/FO76 classic; 7/8 = FO4 Next-Gen / FO76 (same on-disk layout as 1).
# 2–3 = Starfield only (8 extra bytes after the 24-byte base; v3 adds 4-byte compression id).
VERSION_FO4          = 1
VERSION_SF           = 2  # Starfield GNRL/DX10 (use with Create dialog)

def _gnrl_record_size(version: int) -> int:
    """Bytes per GNRL file record (FO4/FO76 v1,v7,v8: 32-bit name hash; Starfield v2–v3: 64-bit)."""
    return 40 if version in (2, 3) else 36


def _dx10_record_head_size(version: int) -> int:
    """Bytes per DX10 file record before chunk list (same 32 vs 64-bit name hash rule)."""
    return 28 if version in (2, 3) else 24

# DXGI_FORMAT numeric ids (same as Windows / Sharp.BSA.BA2 DDS.cs DXGI_FORMAT_FULL).
DXGI_FORMATS = {
    0: "UNKNOWN", 2: "R32G32B32A32_FLOAT",
    10: "R16G16B16A16_FLOAT", 11: "R16G16B16A16_UNORM",
    28: "R8G8B8A8_UNORM", 29: "R8G8B8A8_UNORM_SRGB", 31: "R8G8B8A8_SNORM",
    61: "R8_UNORM",
    49: "R8G8_UNORM",
    71: "BC1_UNORM", 72: "BC1_UNORM_SRGB",
    74: "BC2_UNORM", 75: "BC2_UNORM_SRGB",
    77: "BC3_UNORM", 78: "BC3_UNORM_SRGB",
    80: "BC4_UNORM", 83: "BC5_UNORM", 84: "BC5_SNORM",
    85: "B5G6R5_UNORM",
    87: "B8G8R8A8_UNORM", 88: "B8G8R8X8_UNORM", 91: "B8G8R8A8_UNORM_SRGB",
    95: "BC6H_UF16", 96: "BC6H_SF16",
    98: "BC7_UNORM", 99: "BC7_UNORM_SRGB",
}


def _dds_make_fourcc(s: bytes) -> int:
    return s[0] | (s[1] << 8) | (s[2] << 16) | (s[3] << 24)

# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class BA2Chunk:
    __slots__ = ["offset", "packed_size", "unpacked_size", "start_mip", "end_mip"]
    def __init__(self):
        self.offset = self.packed_size = self.unpacked_size = 0
        self.start_mip = self.end_mip = 0


class BA2FileEntry:
    def __init__(self):
        self.name           = ""
        self.name_hash      = 0
        self.ext            = ""
        self.dir_hash       = 0
        self.flags          = 0
        self.offset         = 0
        self.packed_size    = 0
        self.unpacked_size  = 0
        self.is_compressed  = False
        self.is_texture     = False
        self.width          = 0
        self.height         = 0
        self.num_mips       = 0
        self.dxgi_format    = 0
        self.is_cubemap     = False
        self.tile_mode      = 0
        self.chunks: List[BA2Chunk] = []

    @property
    def directory(self):
        parts = self.name.replace("\\", "/").split("/")
        return "/".join(parts[:-1]) if len(parts) > 1 else ""

    @property
    def filename(self):
        return self.name.replace("\\", "/").split("/")[-1]

    @property
    def format_name(self):
        return DXGI_FORMATS.get(self.dxgi_format, f"FMT_{self.dxgi_format}") if self.is_texture else ""


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def fnv_hash(s: str) -> int:
    h = 0x811C9DC5
    for c in s.lower().encode("utf-8"):
        h = ((h ^ c) * 0x01000193) & 0xFFFFFFFF
    return h


def file_hashes(name: str):
    name = name.replace("/", "\\").lower()
    dot  = name.rfind(".")
    ext  = name[dot+1:] if dot >= 0 else ""
    base = name[:dot]   if dot >= 0 else name
    sl   = base.rfind("\\")
    fname  = base[sl+1:] if sl >= 0 else base
    folder = base[:sl]   if sl >= 0 else ""
    return fnv_hash(fname + "." + ext), fnv_hash(folder)


def _archive_version_label(ver: int) -> str:
    """Human-readable BA2 header version line for the Archive Info tab."""
    if ver == 1:
        return "Fallout 4 / Fallout 76 (v1)"
    if ver in (7, 8):
        return f"Fallout 4 NG / Fallout 76 (v{ver}, same layout as v1)"
    if ver in (2, 3):
        return f"Starfield (v{ver})"
    return f"BA2 v{ver}"


# ─────────────────────────────────────────────────────────────────────────────
# BA2 READER
# ─────────────────────────────────────────────────────────────────────────────

class BA2Archive:
    def __init__(self):
        self.path         = ""
        self.version      = VERSION_FO4
        self.archive_type = ARCHIVE_TYPE_GENERAL
        self.files: List[BA2FileEntry] = []
        self.is_open      = False
        self._fh          = None
        self._io_lock     = threading.Lock()
        self._file_size   = 0
        # Starfield BA2 v2+ extra header (see _parse / Wrye Bash StarfieldBa2Header).
        self.sf_header_unknown1 = 0
        self.sf_header_unknown2 = 0
        self.sf_compression_type = 0

    @property
    def is_texture(self):
        return self.archive_type == ARCHIVE_TYPE_DX10

    @property
    def type_name(self):
        return "DX10 — Textures" if self.is_texture else "General"

    @property
    def total_size(self):
        return sum(e.unpacked_size for e in self.files)

    def open(self, path: str):
        self.close()
        self.path = path
        self._file_size = os.path.getsize(path)
        self._fh  = open(path, "rb")
        try:
            self._parse()
        except Exception:
            self.close()
            raise
        self.is_open = True

    def close(self):
        with self._io_lock:
            if self._fh:
                try:
                    self._fh.close()
                except OSError:
                    pass
                self._fh = None
        self.is_open = False
        self.files   = []
        self._file_size = 0
        self.sf_header_unknown1 = 0
        self.sf_header_unknown2 = 0
        self.sf_compression_type = 0

    def release_handle(self):
        """Close the OS file handle so the archive can be renamed/replaced on Windows."""
        with self._io_lock:
            if self._fh:
                try:
                    self._fh.close()
                except OSError:
                    pass
                self._fh = None

    def reopen_handle(self):
        """Re-open after release_handle if the same BA2Archive instance must keep reading."""
        with self._io_lock:
            if self._fh is None and self.path:
                self._fh = open(self.path, "rb")

    def _parse(self):
        f = self._fh
        f.seek(0)
        magic = f.read(4)
        if magic != MAGIC:
            raise ValueError(f"Not a valid BA2 file (magic bytes: {magic!r})")
        self.version,       = struct.unpack("<I", f.read(4))
        self.archive_type    = f.read(4)
        file_count,         = struct.unpack("<I", f.read(4))
        name_table_offset,  = struct.unpack("<Q", f.read(8))

        # Only Starfield v2/v3 append data after the 24-byte base header (Wrye Bash
        # StarfieldBa2Header). FO4 Next-Gen and Fallout 76 use v7/v8 with the SAME
        # layout as v1 — if we skip bytes for v7, the file table is corrupted.
        self.sf_header_unknown1 = 0
        self.sf_header_unknown2 = 0
        self.sf_compression_type = 0
        if self.version == 2:
            self.sf_header_unknown1, self.sf_header_unknown2 = struct.unpack("<II", f.read(8))
        elif self.version == 3:
            self.sf_header_unknown1, self.sf_header_unknown2 = struct.unpack("<II", f.read(8))
            self.sf_compression_type, = struct.unpack("<I", f.read(4))

        if self.archive_type == ARCHIVE_TYPE_GENERAL:
            self._parse_general(f, file_count, name_table_offset)
        elif self.archive_type == ARCHIVE_TYPE_DX10:
            self._parse_dx10(f, file_count, name_table_offset)
        else:
            raise ValueError(f"Unknown archive type: {self.archive_type!r}")

    def _read_name_table(self, f, offset, count):
        f.seek(offset)
        names = []
        for _ in range(count):
            length, = struct.unpack("<H", f.read(2))
            names.append(f.read(length).decode("utf-8", errors="replace"))
        return names

    def _parse_general(self, f, file_count, name_table_offset):
        entries = []
        for _ in range(file_count):
            e = BA2FileEntry()
            if self.version in (2, 3):
                e.name_hash, = struct.unpack("<Q", f.read(8))
            else:
                e.name_hash, = struct.unpack("<I", f.read(4))
            e.ext             = f.read(4).rstrip(b"\x00").decode("ascii", errors="replace")
            e.dir_hash,      = struct.unpack("<I", f.read(4))
            e.flags,         = struct.unpack("<I", f.read(4))
            e.offset,        = struct.unpack("<Q", f.read(8))
            e.packed_size,   = struct.unpack("<I", f.read(4))
            e.unpacked_size, = struct.unpack("<I", f.read(4))
            _,               = struct.unpack("<I", f.read(4))   # align
            e.is_compressed   = e.packed_size != 0
            entries.append(e)
        for e, n in zip(entries, self._read_name_table(f, name_table_offset, file_count)):
            e.name = n
        self.files = entries

    def _parse_dx10(self, f, file_count, name_table_offset):
        entries = []
        for _ in range(file_count):
            e = BA2FileEntry()
            e.is_texture      = True
            if self.version in (2, 3):
                e.name_hash, = struct.unpack("<Q", f.read(8))
            else:
                e.name_hash, = struct.unpack("<I", f.read(4))
            e.ext             = f.read(4).rstrip(b"\x00").decode("ascii", errors="replace")
            e.dir_hash,      = struct.unpack("<I", f.read(4))
            _,               = struct.unpack("<B", f.read(1))   # unknown_tex
            num_chunks,      = struct.unpack("<B", f.read(1))
            _,               = struct.unpack("<H", f.read(2))   # chunk_header_size (24)
            e.height,        = struct.unpack("<H", f.read(2))
            e.width,         = struct.unpack("<H", f.read(2))
            e.num_mips,      = struct.unpack("<B", f.read(1))
            e.dxgi_format,   = struct.unpack("<B", f.read(1))
            cube_maps,      = struct.unpack("<H", f.read(2))
            e.is_cubemap      = bool(cube_maps & 0xFF)
            e.tile_mode       = (cube_maps >> 8) & 0xFF
            for _ in range(num_chunks):
                c = BA2Chunk()
                c.offset,        = struct.unpack("<Q", f.read(8))
                c.packed_size,   = struct.unpack("<I", f.read(4))
                c.unpacked_size, = struct.unpack("<I", f.read(4))
                c.start_mip,     = struct.unpack("<H", f.read(2))
                c.end_mip,       = struct.unpack("<H", f.read(2))
                _,               = struct.unpack("<I", f.read(4))  # align
                e.chunks.append(c)
            if e.chunks:
                e.unpacked_size = sum(c.unpacked_size for c in e.chunks)
                e.packed_size   = sum(c.packed_size   for c in e.chunks if c.packed_size)
                e.offset        = e.chunks[0].offset
                e.is_compressed = any(c.packed_size for c in e.chunks)
            entries.append(e)
        for e, n in zip(entries, self._read_name_table(f, name_table_offset, file_count)):
            e.name = n
        self.files = entries

    # ── Extraction ───────────────────────────────────────────────────────────

    def extract_file(self, entry: BA2FileEntry, out_dir: str) -> str:
        out_path = os.path.join(out_dir, entry.name.replace("\\", os.sep))
        parent = os.path.dirname(out_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self._io_lock:
            if not self._fh:
                raise OSError("Archive is not open (no file handle).")
            with open(out_path, "wb") as out:
                if entry.is_texture:
                    self._extract_texture(entry, out)
                else:
                    self._extract_general(entry, out)
        return out_path

    def _extract_general(self, e: BA2FileEntry, out):
        if e.offset > self._file_size:
            raise ValueError(
                f"Invalid offset {e.offset} for {e.name!r} (archive is {self._file_size} bytes). "
                "The archive may be corrupt or an unsupported BA2 layout."
            )
        self._fh.seek(e.offset)
        if e.is_compressed:
            packed = self._fh.read(e.packed_size)
            if len(packed) != e.packed_size:
                raise OSError(
                    f"Short read for {e.name!r}: got {len(packed)} bytes, expected {e.packed_size}"
                )
            out.write(zlib.decompress(packed))
        else:
            raw = self._fh.read(e.unpacked_size)
            if len(raw) != e.unpacked_size:
                raise OSError(
                    f"Short read for {e.name!r}: got {len(raw)} bytes, expected {e.unpacked_size}"
                )
            out.write(raw)

    def _extract_texture(self, e: BA2FileEntry, out):
        out.write(self._build_dds_header(e))
        for chunk in e.chunks:
            if chunk.offset > self._file_size:
                raise ValueError(
                    f"Invalid chunk offset {chunk.offset} for {e.name!r} "
                    f"(archive is {self._file_size} bytes)."
                )
            self._fh.seek(chunk.offset)
            if chunk.packed_size:
                packed = self._fh.read(chunk.packed_size)
                if len(packed) != chunk.packed_size:
                    raise OSError(
                        f"Short read for chunk in {e.name!r}: got {len(packed)} bytes, "
                        f"expected {chunk.packed_size}"
                    )
                out.write(zlib.decompress(packed))
            else:
                raw = self._fh.read(chunk.unpacked_size)
                if len(raw) != chunk.unpacked_size:
                    raise OSError(
                        f"Short read for chunk in {e.name!r}: got {len(raw)} bytes, "
                        f"expected {chunk.unpacked_size}"
                    )
                out.write(raw)

    def _build_dds_header(self, e: BA2FileEntry) -> bytes:
        """Build a DDS header compatible with common loaders (aligned with BSA Browser / Sharp.BSA.BA2)."""
        # https://github.com/AlexxEG/BSA_Browser/blob/master/Sharp.BSA.BA2/BA2Util/BA2TextureEntry.cs
        DDS_FOURCC = 0x4
        DDS_RGB = 0x40
        DDS_RGBA = 0x41
        DDS_HDR_TEXTURE = 0x1007   # caps | height | width | pixfmt
        DDS_HDR_MIPMAP = 0x20000
        DDS_HDR_LINEARSIZE = 0x80000
        DDS_HDR_PITCH = 0x8
        DDS_SURFACE_TEXTURE = 0x1000
        DDS_SURFACE_COMPLEX = 0x8
        DDS_SURFACE_MIPMAP = 0x400000
        DDS_MISC_TEXTURECUBE = 4
        DDS_ALPHA_UNKNOWN = 0
        D3D10_RESOURCE_DIMENSION_TEXTURE2D = 3
        # caps2 cubemap flags (same bitmask stack as BSA Browser)
        CUBEMAP_CAPS2 = (
            0x200 | 0x400 | 0x800 | 0x1000 | 0x2000 | 0x4000 | 0x8000 | 0xFC00
        )

        def pf_raw(flags: int, fourcc: int, rgb_bits: int, rm: int, gm: int, bm: int, am: int) -> bytes:
            return struct.pack("<IIIIIIII", 32, flags, fourcc, rgb_bits, rm, gm, bm, am)

        fmt = int(e.dxgi_format)
        w, h = max(1, int(e.width)), max(1, int(e.height))
        mips = max(1, int(e.num_mips or 1))
        cube = bool(e.is_cubemap)
        ver = int(self.version)

        hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
        depth = 1
        mip_count = mips
        pitch_or_linear = 0
        pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
        need_dxt10 = False
        misc = DDS_MISC_TEXTURECUBE if cube else 0

        # --- Pixel format + pitch / flags (mirror BSA Browser WriteHeader) ---
        if fmt == 71:  # BC1_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DXT1"), 0, 0, 0, 0, 0)
            pitch_or_linear = (w * h) // 2
        elif fmt == 74:  # BC2_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DXT3"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * h
        elif fmt == 83:  # BC5_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"BC5U"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * h
        elif fmt == 72:  # BC1_UNORM_SRGB
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            pitch_or_linear = (w * h) // 2
            need_dxt10 = True
        elif fmt in (78, 99, 2):  # BC3_UNORM_SRGB, BC7_UNORM_SRGB, R32G32B32A32_FLOAT
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * h
            need_dxt10 = True
        elif fmt == 28:  # R8G8B8A8_UNORM
            hdr_flags = 0x2100F
            pf = pf_raw(DDS_RGBA, 0, 32, 0x000000FF, 0x0000FF00, 0x00FF0000, 0xFF000000)
            pitch_or_linear = w * 4
        elif fmt == 85:  # B5G6R5_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_RGB, 0, 16, 0x0000F800, 0x000007E0, 0x0000001F, 0)
            pitch_or_linear = w * h * 2
        elif fmt == 88:  # B8G8R8X8_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_RGBA, 0, 32, 0x00FF0000, 0x0000FF00, 0x000000FF, 0xFF000000)
            pitch_or_linear = w * h * 4
        elif fmt == 10:  # R16G16B16A16_FLOAT
            hdr_flags = 0x2100F
            pf = pf_raw(DDS_FOURCC, 0x71, 0, 0, 0, 0, 0)
            pitch_or_linear = w * 8
        elif fmt == 11:  # R16G16B16A16_UNORM
            hdr_flags = 0x2100F
            pf = pf_raw(DDS_FOURCC, 0x24, 0, 0, 0, 0, 0)
            pitch_or_linear = w * 8
        elif fmt == 29:  # R8G8B8A8_UNORM_SRGB
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * 4
            need_dxt10 = True
        elif fmt == 61:  # R8_UNORM
            hdr_flags = 0x2100F
            pf = pf_raw(0x20000, 0, 8, 0xFF, 0, 0, 0)
            pitch_or_linear = w
        elif fmt == 31:  # R8G8B8A8_SNORM (BSA uses 0x80000 in pixel dwFlags)
            hdr_flags = 0x2100F
            pf = pf_raw(0x80000, 0, 32, 0x000000FF, 0x0000FF00, 0x00FF0000, 0xFF000000)
            pitch_or_linear = w * 4
        elif fmt == 87:  # B8G8R8A8_UNORM
            hdr_flags = 0x2100F
            pf = pf_raw(DDS_RGBA, 0, 32, 0x00FF0000, 0x0000FF00, 0x000000FF, 0xFF000000)
            pitch_or_linear = w * 4
        elif fmt == 77:  # BC3_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DXT5"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * h
        elif fmt == 80:  # BC4_UNORM
            hdr_flags = 0xA1007
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"BC4U"), 0, 0, 0, 0, 0)
            pitch_or_linear = max(1, w // 4) * max(1, h // 4) * 8
        elif fmt == 84:  # BC5_SNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"BC5S"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * h
        elif fmt == 95:  # BC6H_UF16
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            nbw = (w + 3) // 4
            nbh = (h + 3) // 4
            pitch_or_linear = nbw * nbh * 16
            need_dxt10 = True
        elif fmt == 96:  # BC6H_SF16 (same header pattern as UF16)
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            nbw = (w + 3) // 4
            nbh = (h + 3) // 4
            pitch_or_linear = nbw * nbh * 16
            need_dxt10 = True
        elif fmt == 98:  # BC7_UNORM
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            nbw = (w + 3) // 4
            nbh = (h + 3) // 4
            pitch_or_linear = nbw * nbh * 16
            need_dxt10 = True
        elif fmt == 91:  # B8G8R8A8_UNORM_SRGB
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            pitch_or_linear = w * 4
            need_dxt10 = True
        else:
            # Unknown: emit DX10 extended header with block / bpp heuristics (best-effort).
            hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_LINEARSIZE | DDS_HDR_MIPMAP
            block_sizes = {71: 8, 72: 8, 74: 16, 75: 16, 77: 16, 78: 16, 80: 8, 83: 16, 84: 16, 95: 16, 96: 16, 98: 16, 99: 16}
            block_fmts = set(block_sizes)
            if fmt in block_fmts:
                bs = block_sizes[fmt]
                nbw = (w + 3) // 4
                nbh = (h + 3) // 4
                pitch_or_linear = nbw * nbh * bs
            else:
                hdr_flags = DDS_HDR_TEXTURE | DDS_HDR_PITCH | DDS_HDR_MIPMAP
                bpp = {49: 2, 61: 1, 10: 8, 11: 8, 28: 4, 29: 4, 31: 4, 87: 4, 88: 4, 91: 4}.get(fmt, 4)
                pitch_or_linear = w * bpp
            pf = pf_raw(DDS_FOURCC, _dds_make_fourcc(b"DX10"), 0, 0, 0, 0, 0)
            need_dxt10 = True

        caps1 = DDS_SURFACE_TEXTURE
        if mips > 1:
            caps1 |= DDS_SURFACE_COMPLEX | DDS_SURFACE_MIPMAP
        elif cube:
            caps1 |= DDS_SURFACE_COMPLEX

        caps2 = CUBEMAP_CAPS2 if cube else 0
        if ver == 7 and mips > 1 and cube:
            caps1 |= 0xFE00
            caps2 = 0

        out = bytearray()
        out += b"DDS "
        out += struct.pack("<IIIIIII", 124, hdr_flags, h, w, pitch_or_linear, depth, mip_count)
        out += b"\x00" * 44
        out += pf
        out += struct.pack("<IIIII", caps1, caps2, 0, 0, 0)

        if need_dxt10:
            out += struct.pack(
                "<IIIII",
                fmt,
                D3D10_RESOURCE_DIMENSION_TEXTURE2D,
                misc,
                1,
                DDS_ALPHA_UNKNOWN,
            )
        elif int(e.tile_mode) != 8:
            # Xbox tile mode: BSA appends DXT10 + tail; without full swizzle data, emit minimal DXT10.
            out += struct.pack(
                "<IIIII",
                fmt,
                D3D10_RESOURCE_DIMENSION_TEXTURE2D,
                misc,
                1,
                DDS_ALPHA_UNKNOWN,
            )

        return bytes(out)

    def get_raw_data(self, e: BA2FileEntry) -> bytes:
        import io
        buf = io.BytesIO()
        with self._io_lock:
            if not self._fh:
                raise OSError("Archive is not open (no file handle).")
            if e.is_texture:
                self._extract_texture(e, buf)
            else:
                self._extract_general(e, buf)
        return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# BA2 WRITER
# ─────────────────────────────────────────────────────────────────────────────

# When the pack root is one of these folders (e.g. …\textures), game paths start with
# textures\… not actors\… — prepend the folder name unless rel already includes it.
_BA2_PATH_PREFIX_ROOTS = frozenset({
    "textures", "meshes", "interface", "music", "sound", "scripts", "strings", "video",
    "materials", "lodsettings", "vis", "programs", "animationdatasinglefile",
    "shadersfx", "trees", "fonts", "misc", "sky", "grass", "effects",
})


class BA2Writer:

    @staticmethod
    def create(out_path, source_files, archive_type, root, compress, version, progress_cb=None):
        if archive_type == ARCHIVE_TYPE_GENERAL:
            BA2Writer._write_general(out_path, source_files, root, compress, version, progress_cb)
        else:
            BA2Writer._write_dx10(out_path, source_files, root, compress, version, progress_cb)

    @staticmethod
    def internal_name(path, root):
        root = os.path.normpath(os.path.abspath(root))
        path = os.path.normpath(os.path.abspath(path))
        try:
            rel = os.path.relpath(path, root).replace("/", "\\")
        except ValueError:
            return os.path.basename(path).replace("/", "\\")
        head = os.path.basename(root.rstrip(os.sep + "/"))
        if (
            head
            and head.lower() in _BA2_PATH_PREFIX_ROOTS
            and not rel.lower().startswith(head.lower() + "\\")
        ):
            return f"{head}\\{rel}"
        return rel

    @staticmethod
    def _header_data_size(version: int) -> int:
        """Total bytes from file start through BA2 header (before file record table)."""
        n = 24
        if version == 2:
            n += 8
        elif version == 3:
            n += 12
        return n

    @staticmethod
    def _write_header_prefix(
        f,
        version: int,
        archive_type_b: bytes,
        file_count: int,
        name_table_offset: int,
        unk_pair=(0, 0),
        compression: int = 0,
    ):
        f.write(MAGIC)
        f.write(struct.pack("<I", version))
        f.write(archive_type_b)
        f.write(struct.pack("<I", file_count))
        f.write(struct.pack("<Q", name_table_offset))
        if version == 2:
            f.write(struct.pack("<II", int(unk_pair[0]), int(unk_pair[1])))
        elif version == 3:
            f.write(struct.pack("<II", int(unk_pair[0]), int(unk_pair[1])))
            f.write(struct.pack("<I", int(compression)))

    @staticmethod
    def _write_general(out_path, source_files, root, compress, version, pcb):
        entries, blobs = [], []
        total = len(source_files)
        for i, src in enumerate(source_files):
            if pcb: pcb(i, total, os.path.basename(src))
            name = BA2Writer.internal_name(src, root)
            with open(src, "rb") as f:
                raw = f.read()
            packed = b""
            if compress and raw:
                c = zlib.compress(raw, 9)
                if len(c) < len(raw):
                    packed = c
            e = BA2FileEntry()
            e.name         = name
            e.ext          = Path(src).suffix.lstrip(".").upper()[:4]
            e.name_hash, e.dir_hash = file_hashes(name)
            e.unpacked_size = len(raw)
            e.packed_size   = len(packed) if packed else 0
            entries.append(e)
            blobs.append(packed if packed else raw)

        rsz = _gnrl_record_size(version)
        hdr = BA2Writer._header_data_size(version)
        cur = hdr + rsz * len(entries)
        for e, b in zip(entries, blobs):
            e.offset = cur; cur += len(b)
        nto = cur

        with open(out_path, "wb") as f:
            BA2Writer._write_header_prefix(
                f, version, ARCHIVE_TYPE_GENERAL, len(entries), nto, (0, 0), 0
            )
            for e in entries:
                if version in (2, 3):
                    f.write(struct.pack("<Q", e.name_hash))
                else:
                    f.write(struct.pack("<I", e.name_hash & 0xFFFFFFFF))
                f.write(e.ext.encode("ascii")[:4].ljust(4, b"\x00"))
                f.write(struct.pack("<I", e.dir_hash))
                f.write(struct.pack("<I", 0))
                f.write(struct.pack("<Q", e.offset))
                f.write(struct.pack("<I", e.packed_size))
                f.write(struct.pack("<I", e.unpacked_size))
                f.write(struct.pack("<I", 0xBAADF00D))
            for b in blobs:
                f.write(b)
            for e in entries:
                n = e.name.encode("utf-8")
                f.write(struct.pack("<H", len(n))); f.write(n)
        if pcb: pcb(total, total, "Done")

    @staticmethod
    def _write_dx10(out_path, source_files, root, compress, version, pcb):
        processed = []
        total = len(source_files)
        for i, src in enumerate(source_files):
            if pcb: pcb(i, total, os.path.basename(src))
            name = BA2Writer.internal_name(src, root)
            try:
                e, chunks_data = BA2Writer._process_dds(src, name, compress)
                processed.append((e, chunks_data))
            except Exception as ex:
                print(f"  Skipping {src}: {ex}")

        dhs = _dx10_record_head_size(version)
        fts = sum(dhs + 24 * len(e.chunks) for e, _ in processed)
        hdr = BA2Writer._header_data_size(version)
        cur = hdr + fts
        for e, cdata in processed:
            for j, chunk in enumerate(e.chunks):
                chunk.offset = cur; cur += len(cdata[j])
        nto = cur

        with open(out_path, "wb") as f:
            BA2Writer._write_header_prefix(
                f, version, ARCHIVE_TYPE_DX10, len(processed), nto, (0, 0), 0
            )
            for e, _ in processed:
                if version in (2, 3):
                    f.write(struct.pack("<Q", e.name_hash))
                else:
                    f.write(struct.pack("<I", e.name_hash & 0xFFFFFFFF))
                f.write(e.ext.encode("ascii")[:4].ljust(4, b"\x00"))
                f.write(struct.pack("<I", e.dir_hash))
                f.write(struct.pack("<B", 0))  # unknown_tex
                f.write(struct.pack("<B", len(e.chunks)))
                f.write(struct.pack("<H", 24))
                f.write(struct.pack("<H", e.height))
                f.write(struct.pack("<H", e.width))
                f.write(struct.pack("<B", e.num_mips))
                f.write(struct.pack("<B", e.dxgi_format))
                cmh = ((e.tile_mode & 0xFF) << 8) | (1 if e.is_cubemap else 0)
                f.write(struct.pack("<H", cmh))
                for chunk in e.chunks:
                    f.write(struct.pack("<Q", chunk.offset))
                    f.write(struct.pack("<I", chunk.packed_size))
                    f.write(struct.pack("<I", chunk.unpacked_size))
                    f.write(struct.pack("<H", chunk.start_mip))
                    f.write(struct.pack("<H", chunk.end_mip))
                    f.write(struct.pack("<I", 0xBAADF00D))
            for _, cdata in processed:
                for b in cdata: f.write(b)
            for e, _ in processed:
                n = e.name.encode("utf-8")
                f.write(struct.pack("<H", len(n))); f.write(n)
        if pcb: pcb(total, total, "Done")

    @staticmethod
    def _process_dds(path, name, compress):
        with open(path, "rb") as f:
            magic = f.read(4)
            if magic != b"DDS ":
                raise ValueError("Not a DDS file")
            f.read(4)  # hdr size
            f.read(4)  # flags
            height, = struct.unpack("<I", f.read(4))
            width,  = struct.unpack("<I", f.read(4))
            f.read(4)  # pitch
            f.read(4)  # depth
            mips,   = struct.unpack("<I", f.read(4))
            f.read(44) # reserved
            f.read(4)  # pf size
            f.read(4)  # pf flags
            pf_fourcc = f.read(4)
            f.read(20) # rest of pf
            f.read(20) # caps + reserved2
            dxgi_fmt = 28
            is_cubemap = False
            if pf_fourcc == b"DX10":
                dxgi_fmt, = struct.unpack("<I", f.read(4))
                f.read(4)   # dim
                misc, = struct.unpack("<I", f.read(4))
                f.read(8)   # array, misc2
                is_cubemap = bool(misc & 0x4)
            pixel_data = f.read()

        e = BA2FileEntry()
        e.name        = name
        e.ext         = "dds"
        e.name_hash, e.dir_hash = file_hashes(name)
        e.width       = width
        e.height      = height
        e.num_mips    = max(1, mips)
        e.dxgi_format = dxgi_fmt
        e.is_cubemap  = is_cubemap
        e.is_texture  = True
        e.tile_mode   = 8

        chunk = BA2Chunk()
        chunk.start_mip     = 0
        chunk.end_mip       = max(0, e.num_mips - 1)
        chunk.unpacked_size = len(pixel_data)
        if compress and pixel_data:
            packed = zlib.compress(pixel_data, 9)
            if len(packed) < len(pixel_data):
                chunk.packed_size = len(packed)
                cdata = [packed]
            else:
                chunk.packed_size = 0
                cdata = [pixel_data]
        else:
            chunk.packed_size = 0
            cdata = [pixel_data]
        e.chunks        = [chunk]
        e.unpacked_size = len(pixel_data)
        e.packed_size   = chunk.packed_size
        return e, cdata


# ─────────────────────────────────────────────────────────────────────────────
# WORKER THREADS
# ─────────────────────────────────────────────────────────────────────────────

class ExtractWorker(QThread):
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(bool, str)

    def __init__(self, archive, entries, out_dir):
        super().__init__()
        self.archive = archive
        self.entries = entries
        self.out_dir = out_dir

    def run(self):
        try:
            total = len(self.entries)
            for i, e in enumerate(self.entries):
                self.progress.emit(i, total, e.name)
                self.archive.extract_file(e, self.out_dir)
            self.finished.emit(True, f"Extracted {total} file(s) to:\n{self.out_dir}")
        except Exception as ex:
            self.finished.emit(False, str(ex))


class CreateWorker(QThread):
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(bool, str)

    def __init__(self, out_path, source_files, archive_type, root, compress, version):
        super().__init__()
        self.out_path = out_path; self.source_files = source_files
        self.archive_type = archive_type; self.root = root
        self.compress = compress; self.version = version

    def run(self):
        try:
            BA2Writer.create(self.out_path, self.source_files, self.archive_type,
                             self.root, self.compress, self.version,
                             lambda c, t, n: self.progress.emit(c, t, n))
            self.finished.emit(True, f"Archive created:\n{self.out_path}")
        except Exception as ex:
            self.finished.emit(False, str(ex))


class RebuildWorker(QThread):
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(bool, str)

    def __init__(self, archive, pending_adds, deleted, replacements, out_path):
        super().__init__()
        self.archive = archive; self.pending_adds = pending_adds
        self.deleted = deleted; self.replacements = replacements
        self.out_path = out_path

    def run(self):
        released = False
        ok = False
        try:
            tmp = self.out_path + ".tmp"
            self._rebuild(tmp)
            # Windows keeps the .ba2 locked while our read handle is open; close before rename.
            self.archive.release_handle()
            released = True
            bak = self.out_path + ".bak"
            if os.path.exists(self.out_path):
                if os.path.exists(bak):
                    os.remove(bak)
                os.rename(self.out_path, bak)
            os.rename(tmp, self.out_path)
            ok = True
            self.finished.emit(True, f"Archive saved:\n{self.out_path}")
        except Exception as ex:
            self.finished.emit(False, str(ex))
        finally:
            if released and not ok:
                try:
                    self.archive.reopen_handle()
                except OSError:
                    pass

    def _rebuild(self, out_path):
        arc = self.archive
        final = []  # (entry, src_or_None)
        for e in arc.files:
            if e.name in self.deleted: continue
            if e.name in self.replacements:
                final.append((e, self.replacements[e.name]))
            else:
                final.append((e, None))
        for iname, src in self.pending_adds.items():
            ne = BA2FileEntry()
            ne.name = iname
            ne.ext  = Path(src).suffix.lstrip(".").upper()[:4]
            ne.name_hash, ne.dir_hash = file_hashes(iname)
            ne.is_texture = arc.is_texture
            final.append((ne, src))

        total = len(final)
        if arc.is_texture:
            self._rebuild_dx10(out_path, final, total)
        else:
            self._rebuild_general(out_path, final, total)

    def _rebuild_general(self, out_path, final, total):
        entries, blobs = [], []
        for i, (e, src) in enumerate(final):
            self.progress.emit(i, total, e.name)
            if src:
                with open(src, "rb") as sf:
                    raw = sf.read()
            else:
                raw = self.archive.get_raw_data(e)
            packed = zlib.compress(raw, 9)
            if len(packed) >= len(raw): packed = b""
            ne = BA2FileEntry()
            ne.name = e.name; ne.ext = e.ext
            ne.name_hash = e.name_hash; ne.dir_hash = e.dir_hash
            ne.flags = getattr(e, "flags", 0)
            ne.unpacked_size = len(raw)
            ne.packed_size   = len(packed) if packed else 0
            entries.append(ne)
            blobs.append(packed if packed else raw)

        ver = self.archive.version
        rsz = _gnrl_record_size(ver)
        hdr = BA2Writer._header_data_size(ver)
        cur = hdr + rsz * len(entries)
        for en, b in zip(entries, blobs):
            en.offset = cur; cur += len(b)
        nto = cur

        with open(out_path, "wb") as f:
            BA2Writer._write_header_prefix(
                f,
                ver,
                ARCHIVE_TYPE_GENERAL,
                len(entries),
                nto,
                (self.archive.sf_header_unknown1, self.archive.sf_header_unknown2),
                self.archive.sf_compression_type,
            )
            for en in entries:
                if ver in (2, 3):
                    f.write(struct.pack("<Q", en.name_hash))
                else:
                    f.write(struct.pack("<I", en.name_hash & 0xFFFFFFFF))
                f.write(en.ext.encode("ascii")[:4].ljust(4, b"\x00"))
                f.write(struct.pack("<I", en.dir_hash))
                f.write(struct.pack("<I", en.flags))
                f.write(struct.pack("<Q", en.offset))
                f.write(struct.pack("<I", en.packed_size))
                f.write(struct.pack("<I", en.unpacked_size))
                f.write(struct.pack("<I", 0xBAADF00D))
            for b in blobs: f.write(b)
            for en in entries:
                n = en.name.encode("utf-8")
                f.write(struct.pack("<H", len(n))); f.write(n)

    def _rebuild_dx10(self, out_path, final, total):
        processed = []
        for i, (e, src) in enumerate(final):
            self.progress.emit(i, total, e.name)
            if src:
                ne, cdata = BA2Writer._process_dds(src, e.name, True)
                processed.append((ne, cdata))
            else:
                blobs = []
                with self.archive._io_lock:
                    if not self.archive._fh:
                        raise OSError("Archive is not open (no file handle).")
                    for chunk in e.chunks:
                        self.archive._fh.seek(chunk.offset)
                        if chunk.packed_size:
                            b = self.archive._fh.read(chunk.packed_size)
                            if len(b) != chunk.packed_size:
                                raise OSError(
                                    f"Short read for chunk in {e.name!r}: got {len(b)} bytes, "
                                    f"expected {chunk.packed_size}"
                                )
                            blobs.append(b)
                        else:
                            b = self.archive._fh.read(chunk.unpacked_size)
                            if len(b) != chunk.unpacked_size:
                                raise OSError(
                                    f"Short read for chunk in {e.name!r}: got {len(b)} bytes, "
                                    f"expected {chunk.unpacked_size}"
                                )
                            blobs.append(b)
                processed.append((e, blobs))

        ver = self.archive.version
        dhs = _dx10_record_head_size(ver)
        fts = sum(dhs + 24 * len(e.chunks) for e, _ in processed)
        hdr = BA2Writer._header_data_size(ver)
        cur = hdr + fts
        for e, cdata in processed:
            for j, chunk in enumerate(e.chunks):
                chunk.offset = cur; cur += len(cdata[j])
        nto = cur

        with open(out_path, "wb") as f:
            BA2Writer._write_header_prefix(
                f,
                ver,
                ARCHIVE_TYPE_DX10,
                len(processed),
                nto,
                (self.archive.sf_header_unknown1, self.archive.sf_header_unknown2),
                self.archive.sf_compression_type,
            )
            for e, _ in processed:
                if ver in (2, 3):
                    f.write(struct.pack("<Q", e.name_hash))
                else:
                    f.write(struct.pack("<I", e.name_hash & 0xFFFFFFFF))
                f.write(e.ext.encode("ascii")[:4].ljust(4, b"\x00"))
                f.write(struct.pack("<I", e.dir_hash))
                f.write(struct.pack("<B", 0))
                f.write(struct.pack("<B", len(e.chunks)))
                f.write(struct.pack("<H", 24))
                f.write(struct.pack("<H", e.height))
                f.write(struct.pack("<H", e.width))
                f.write(struct.pack("<B", e.num_mips))
                f.write(struct.pack("<B", e.dxgi_format))
                cmh = ((e.tile_mode & 0xFF) << 8) | (1 if e.is_cubemap else 0)
                f.write(struct.pack("<H", cmh))
                for chunk in e.chunks:
                    f.write(struct.pack("<Q", chunk.offset))
                    f.write(struct.pack("<I", chunk.packed_size))
                    f.write(struct.pack("<I", chunk.unpacked_size))
                    f.write(struct.pack("<H", chunk.start_mip))
                    f.write(struct.pack("<H", chunk.end_mip))
                    f.write(struct.pack("<I", 0xBAADF00D))
            for _, cdata in processed:
                for b in cdata: f.write(b)
            for e, _ in processed:
                n = e.name.encode("utf-8")
                f.write(struct.pack("<H", len(n))); f.write(n)


# ─────────────────────────────────────────────────────────────────────────────
# CREATE ARCHIVE DIALOG
# ─────────────────────────────────────────────────────────────────────────────

class CreateArchiveDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create New BA2 Archive")
        self.setMinimumWidth(580)
        self.source_files = []
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        # Archive type
        tbox = QGroupBox("Archive Type")
        tl = QVBoxLayout(tbox)
        self.rb_gnrl = QRadioButton("General  — meshes, sounds, scripts, strings, etc.")
        self.rb_dx10 = QRadioButton("DX10  — Textures (.dds files only)")
        self.rb_gnrl.setChecked(True)
        tl.addWidget(self.rb_gnrl); tl.addWidget(self.rb_dx10)
        layout.addWidget(tbox)

        # Settings
        sbox = QGroupBox("Settings")
        form = QFormLayout(sbox)
        self.cb_compress = QCheckBox("Compress file data with zlib")
        self.cb_compress.setChecked(True)
        form.addRow("Compression:", self.cb_compress)
        self.cmb_ver = QComboBox()
        self.cmb_ver.addItem("Fallout 4 / Fallout 76  —  BA2 version 1", VERSION_FO4)
        self.cmb_ver.addItem("Starfield  —  BA2 version 2", VERSION_SF)
        form.addRow("Game / archive version:", self.cmb_ver)
        layout.addWidget(sbox)

        # Root folder
        rbox = QGroupBox(
            "Root folder  —  choosing one scans all subfolders; the list shows archive-style paths"
        )
        rl = QHBoxLayout(rbox)
        self.le_root = QLineEdit()
        self.le_root.setPlaceholderText(
            "Browse to your Data subfolder (e.g. …\\textures). The file list fills from here…"
        )
        self.le_root.editingFinished.connect(self._root_editing_finished)
        btn_root = QPushButton("Browse…")
        btn_root.clicked.connect(self._pick_root)
        rl.addWidget(self.le_root); rl.addWidget(btn_root)
        layout.addWidget(rbox)

        self.rb_gnrl.toggled.connect(self._on_archive_type_changed)
        self.rb_dx10.toggled.connect(self._on_archive_type_changed)

        # File list
        fbox = QGroupBox("Files to Pack  (preview — same layout as inside the BA2)")
        fl = QVBoxLayout(fbox)
        br = QHBoxLayout()
        for label, slot in [("Add Files…", self._add_files),
                             ("Add Folder…", self._add_folder),
                             ("Remove Selected", self._remove_sel),
                             ("Clear All", self._clear)]:
            b = QPushButton(label); b.clicked.connect(slot); br.addWidget(b)
        br.addStretch()
        fl.addLayout(br)
        self.file_list = QListWidget()
        self.file_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.file_list.setMinimumHeight(150)
        fl.addWidget(self.file_list)
        self.lbl_cnt = QLabel("0 files")
        fl.addWidget(self.lbl_cnt)
        layout.addWidget(fbox)

        # Output
        obox = QGroupBox("Output BA2 File")
        ol = QHBoxLayout(obox)
        self.le_out = QLineEdit()
        self.le_out.setPlaceholderText("Choose where to save the archive…")
        btn_out = QPushButton("Browse…")
        btn_out.clicked.connect(self._pick_out)
        ol.addWidget(self.le_out); ol.addWidget(btn_out)
        layout.addWidget(obox)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.button(QDialogButtonBox.Ok).setText("Create Archive")
        btns.accepted.connect(self._accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _pick_root(self):
        d = QFileDialog.getExistingDirectory(self, "Select Root Folder")
        if not d:
            return
        self.le_root.setText(os.path.abspath(d))
        self._scan_root_into_sources()

    def _root_editing_finished(self):
        p = self.le_root.text().strip()
        if not p:
            return
        ap = os.path.abspath(p)
        if os.path.isdir(ap):
            self.le_root.setText(ap)
            self._scan_root_into_sources()

    def _on_archive_type_changed(self, checked: bool):
        if not checked:
            return
        p = self.le_root.text().strip()
        if p and os.path.isdir(os.path.abspath(p)):
            self._scan_root_into_sources()

    def _scan_root_into_sources(self):
        """Replace file list with every file under the root (all subfolders, including leaves)."""
        root_txt = self.le_root.text().strip()
        if not root_txt:
            return
        root = os.path.abspath(root_txt)
        if not os.path.isdir(root):
            return
        self.source_files.clear()
        ext = ".dds" if self.rb_dx10.isChecked() else None
        for walk_root, _, files in os.walk(root):
            for fn in files:
                if ext and not fn.lower().endswith(ext):
                    continue
                self.source_files.append(os.path.join(walk_root, fn))
        self._sync_list_from_sources()

    def _archive_preview_path(self, abs_file: str) -> str:
        """Same path rules as inside the BA2 (see BA2Writer.internal_name)."""
        root_txt = self.le_root.text().strip()
        if not root_txt:
            return os.path.normpath(os.path.abspath(abs_file)).replace(os.sep, "\\")
        return BA2Writer.internal_name(abs_file, root_txt)

    def _sync_list_from_sources(self):
        self.file_list.clear()
        for full in self.source_files:
            it = QListWidgetItem(self._archive_preview_path(full))
            it.setData(Qt.UserRole, full)
            it.setToolTip(full)
            self.file_list.addItem(it)
        self._upd()

    def _add_files(self):
        filt = "DDS Textures (*.dds);;All Files (*)" if self.rb_dx10.isChecked() else "All Files (*)"
        files, _ = QFileDialog.getOpenFileNames(self, "Add Files", "", filt)
        for f in files:
            fa = os.path.abspath(f)
            if fa not in self.source_files:
                self.source_files.append(fa)
        self._sync_list_from_sources()

    def _add_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Add Folder")
        if not d:
            return
        d = os.path.abspath(d)
        if not self.le_root.text().strip():
            self.le_root.setText(d)
            self._scan_root_into_sources()
            return
        ext = ".dds" if self.rb_dx10.isChecked() else None
        for root, _, files in os.walk(d):
            for fn in files:
                if ext and not fn.lower().endswith(ext):
                    continue
                full = os.path.abspath(os.path.join(root, fn))
                if full not in self.source_files:
                    self.source_files.append(full)
        self._sync_list_from_sources()

    def _remove_sel(self):
        for item in self.file_list.selectedItems():
            path = item.data(Qt.UserRole)
            if path and path in self.source_files:
                self.source_files.remove(path)
        self._sync_list_from_sources()

    def _clear(self):
        self.source_files.clear()
        self.file_list.clear()
        self.le_root.clear()
        self._upd()

    def _upd(self):
        self.lbl_cnt.setText(f"{len(self.source_files)} file(s)")

    def _pick_out(self):
        p, _ = QFileDialog.getSaveFileName(self, "Save BA2", "", "BA2 Archives (*.ba2)")
        if p: self.le_out.setText(p)

    def _accept(self):
        if not self.source_files:
            QMessageBox.warning(self, "No Files", "Add at least one file to pack."); return
        if not self.le_out.text().strip():
            QMessageBox.warning(self, "No Output", "Choose an output path."); return
        root_txt = self.le_root.text().strip()
        if root_txt:
            ra = os.path.normpath(os.path.abspath(root_txt))
            if not os.path.isdir(ra):
                QMessageBox.warning(self, "Invalid Root", "Root folder is not a valid directory.")
                return
            for f in self.source_files:
                fa = os.path.normpath(os.path.abspath(f))
                if fa != ra and not fa.startswith(ra + os.sep):
                    QMessageBox.warning(
                        self,
                        "Outside Root",
                        "Every file must be under the root folder. Remove extras or pick a higher root.\n\n"
                        + fa,
                    )
                    return
        self.accept()

    def get_settings(self):
        root = self.le_root.text().strip()
        if self.source_files:
            abs_paths = [os.path.normpath(os.path.abspath(f)) for f in self.source_files]
            if not root:
                if len(abs_paths) == 1:
                    root = os.path.dirname(abs_paths[0])
                else:
                    try:
                        root = os.path.commonpath(abs_paths)
                    except ValueError:
                        root = os.path.dirname(abs_paths[0])
            else:
                root = os.path.normpath(os.path.abspath(root))
        elif root:
            root = os.path.normpath(os.path.abspath(root))
        return {
            "archive_type": ARCHIVE_TYPE_DX10 if self.rb_dx10.isChecked() else ARCHIVE_TYPE_GENERAL,
            "compress"    : self.cb_compress.isChecked(),
            "version"     : self.cmb_ver.currentData(),
            "root"        : root,
            "out_path"    : self.le_out.text().strip(),
            "source_files": self.source_files,
        }


# ─────────────────────────────────────────────────────────────────────────────
# ADD FILES DIALOG  (internal path helper)
# ─────────────────────────────────────────────────────────────────────────────

class AddFilesDialog(QDialog):
    def __init__(self, is_texture, parent=None):
        super().__init__(parent)
        self.is_texture = is_texture
        self.source_files = []
        self.setWindowTitle("Add Files to Archive")
        self.setMinimumWidth(500)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        # File picker
        fbox = QGroupBox("Files to Add")
        fl = QVBoxLayout(fbox)
        br = QHBoxLayout()
        btn_f = QPushButton("Add Files…"); btn_f.clicked.connect(self._add_files)
        btn_d = QPushButton("Add Folder…"); btn_d.clicked.connect(self._add_folder)
        btn_c = QPushButton("Clear"); btn_c.clicked.connect(self._clear)
        br.addWidget(btn_f); br.addWidget(btn_d); br.addWidget(btn_c); br.addStretch()
        fl.addLayout(br)
        self.lst = QListWidget()
        self.lst.setMinimumHeight(120)
        fl.addWidget(self.lst)
        self.lbl = QLabel("0 files")
        fl.addWidget(self.lbl)
        layout.addWidget(fbox)

        # Internal prefix
        pbox = QGroupBox("Internal Archive Path Prefix")
        pl = QVBoxLayout(pbox)
        pl.addWidget(QLabel("Folder path inside the archive  (e.g.  textures\\actors\\character)"))
        self.le_prefix = QLineEdit()
        self.le_prefix.setPlaceholderText("Leave blank to put files in the archive root")
        pl.addWidget(self.le_prefix)
        layout.addWidget(pbox)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.button(QDialogButtonBox.Ok).setText("Add to Archive")
        btns.accepted.connect(self._accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _add_files(self):
        filt = "DDS Textures (*.dds);;All Files (*)" if self.is_texture else "All Files (*)"
        files, _ = QFileDialog.getOpenFileNames(self, "Add Files", "", filt)
        for f in files:
            if f not in self.source_files:
                self.source_files.append(f); self.lst.addItem(f)
        self._upd()

    def _add_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Add Folder")
        if not d: return
        ext = ".dds" if self.is_texture else None
        for root, _, files in os.walk(d):
            for fn in files:
                if ext and not fn.lower().endswith(ext): continue
                full = os.path.join(root, fn)
                if full not in self.source_files:
                    self.source_files.append(full); self.lst.addItem(full)
        self._upd()

    def _clear(self):
        self.source_files.clear(); self.lst.clear(); self._upd()

    def _upd(self):
        self.lbl.setText(f"{len(self.source_files)} file(s)")

    def _accept(self):
        if not self.source_files:
            QMessageBox.warning(self, "No Files", "Add at least one file."); return
        self.accept()

    def get_result(self):
        prefix = self.le_prefix.text().strip().replace("/", "\\")
        result = {}
        for src in self.source_files:
            fname = os.path.basename(src)
            iname = (prefix.rstrip("\\") + "\\" + fname).lstrip("\\") if prefix else fname
            result[iname] = src
        return result


# ─────────────────────────────────────────────────────────────────────────────
# PROGRESS DIALOG  (non-modal, stays on screen)
# ─────────────────────────────────────────────────────────────────────────────

class WorkProgressDialog(QDialog):
    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowModality(Qt.WindowModal)
        self.setMinimumWidth(420)
        self.setFixedHeight(130)
        layout = QVBoxLayout(self)
        self.lbl  = QLabel("Starting…")
        self.bar  = QProgressBar()
        self.bar.setRange(0, 100)
        layout.addWidget(self.lbl)
        layout.addWidget(self.bar)

    def update(self, cur, total, name):
        pct = int(cur / total * 100) if total else 100
        self.bar.setValue(pct)
        self.lbl.setText(f"[{cur}/{total}]  {os.path.basename(name)}")
        QApplication.processEvents()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN WINDOW
# ─────────────────────────────────────────────────────────────────────────────

class BA2Manager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.archive     : Optional[BA2Archive] = None
        self.pending_adds: Dict[str, str] = {}
        self.deleted     : set            = set()
        self.replacements: Dict[str, str] = {}
        self.dirty       : bool           = False
        self._worker                      = None

        self.setWindowTitle("BA2 Manager")
        self.setMinimumSize(1050, 680)
        self.resize(1280, 780)

        self._build_menu()
        self._build_toolbar()
        self._build_central()
        self._build_statusbar()
        self._refresh_actions()

    # ── Menu ─────────────────────────────────────────────────────────────────

    def _build_menu(self):
        mb = self.menuBar()

        fm = mb.addMenu("&File")
        self.act_open    = fm.addAction("&Open BA2 Archive…",   self._open_archive,   "Ctrl+O")
        self.act_close   = fm.addAction("&Close Archive",        self._close_archive,  "Ctrl+W")
        fm.addSeparator()
        self.act_new     = fm.addAction("&New Archive…",         self._new_archive,    "Ctrl+N")
        fm.addSeparator()
        self.act_save    = fm.addAction("&Save  (rebuild)",      self._save_archive,   "Ctrl+S")
        self.act_save_as = fm.addAction("Save &As…",             self._save_archive_as,"Ctrl+Shift+S")
        fm.addSeparator()
        fm.addAction("E&xit", self.close, "Alt+F4")

        em = mb.addMenu("&Edit")
        self.act_add    = em.addAction("&Add Files…",           self._add_files)
        self.act_rep    = em.addAction("&Replace Selected…",    self._replace_selected)
        self.act_del    = em.addAction("&Delete Selected",      self._delete_selected,  "Delete")
        em.addSeparator()
        self.act_selall = em.addAction("Select &All",           self._select_all,       "Ctrl+A")

        xm = mb.addMenu("E&xtract")
        self.act_ext_sel = xm.addAction("Extract &Selected…",  self._extract_selected, "Ctrl+E")
        self.act_ext_all = xm.addAction("Extract &All…",       self._extract_all,      "Ctrl+Shift+E")

        vm = mb.addMenu("&View")
        vm.addAction("Expand All Folders",  lambda: self.folder_tree.expandAll())
        vm.addAction("Collapse All Folders",lambda: self.folder_tree.collapseAll())
        vm.addSeparator()
        vm.addAction("Clear &Filter",       self._clear_filter,  "Escape")

        hm = mb.addMenu("&Help")
        hm.addAction("&About", self._about)

    # ── Toolbar ──────────────────────────────────────────────────────────────

    def _build_toolbar(self):
        tb = QToolBar("Tools")
        tb.setMovable(False)
        self.addToolBar(tb)

        def act(label, tip, slot):
            a = QAction(label, self); a.setToolTip(tip); a.triggered.connect(slot)
            tb.addAction(a); return a

        self.tb_open    = act("Open",         "Open BA2 archive",                 self._open_archive)
        self.tb_new     = act("New",          "Create a new BA2 archive",         self._new_archive)
        self.tb_save    = act("Save",         "Rebuild and save archive",         self._save_archive)
        tb.addSeparator()
        self.tb_ext_sel = act("Extract Sel",  "Extract selected files",           self._extract_selected)
        self.tb_ext_all = act("Extract All",  "Extract all files in archive",     self._extract_all)
        tb.addSeparator()
        self.tb_add     = act("Add Files",    "Add files to archive",             self._add_files)
        self.tb_rep     = act("Replace",      "Replace a file inside archive",    self._replace_selected)
        self.tb_del     = act("Delete",       "Delete selected files",            self._delete_selected)
        tb.addSeparator()

        tb.addWidget(QLabel("  Filter: "))
        self.le_filter = QLineEdit()
        self.le_filter.setPlaceholderText("Type to filter filenames…")
        self.le_filter.setFixedWidth(260)
        self.le_filter.textChanged.connect(self._apply_filter)
        tb.addWidget(self.le_filter)

        btn_clr = QToolButton()
        btn_clr.setText("✕")
        btn_clr.setToolTip("Clear filter")
        btn_clr.clicked.connect(self._clear_filter)
        tb.addWidget(btn_clr)

    # ── Central widget ────────────────────────────────────────────────────────

    def _build_central(self):
        central = QWidget()
        self.setCentralWidget(central)
        outer = QHBoxLayout(central)
        outer.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Horizontal)
        outer.addWidget(splitter)

        # Left panel: folder tree
        left = QWidget()
        ll = QVBoxLayout(left)
        ll.setContentsMargins(4, 4, 0, 4)
        lbl = QLabel("Folders")
        lbl.setStyleSheet("font-weight: bold; padding: 2px;")
        ll.addWidget(lbl)
        self.folder_tree = QTreeWidget()
        self.folder_tree.setHeaderHidden(True)
        self.folder_tree.setMinimumWidth(190)
        self.folder_tree.currentItemChanged.connect(self._folder_selected)
        ll.addWidget(self.folder_tree)
        splitter.addWidget(left)

        # Right panel: tabs
        right = QWidget()
        rl = QVBoxLayout(right)
        rl.setContentsMargins(0, 4, 4, 4)
        rl.setSpacing(4)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)

        # ── Tab 1: File list ─────────────────────────────────────────────────
        self.file_table = QTableWidget()
        self.file_table.setColumnCount(7)
        self.file_table.setHorizontalHeaderLabels(
            ["Filename", "Internal Path", "Unpacked Size", "Compressed",
             "DXGI Format", "Dimensions", "Mips"]
        )
        self.file_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.file_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.file_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.file_table.setAlternatingRowColors(True)
        self.file_table.setSortingEnabled(True)
        self.file_table.verticalHeader().setVisible(False)
        self.file_table.verticalHeader().setDefaultSectionSize(22)
        hh = self.file_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.Stretch)
        hh.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.file_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_table.customContextMenuRequested.connect(self._context_menu)
        self.file_table.doubleClicked.connect(self._on_double_click)
        self.tabs.addTab(self.file_table, "Files")

        # ── Tab 2: Archive info ──────────────────────────────────────────────
        self.info_text = QTextEdit()
        self.info_text.setReadOnly(True)
        self.info_text.setFont(QFont("Courier New", 10))
        self.tabs.addTab(self.info_text, "Archive Info")

        # ── Tab 3: Pending changes ───────────────────────────────────────────
        self.pending_text = QTextEdit()
        self.pending_text.setReadOnly(True)
        self.pending_text.setFont(QFont("Courier New", 10))
        self.tabs.addTab(self.pending_text, "Pending Changes (0)")

        rl.addWidget(self.tabs)
        splitter.addWidget(right)
        splitter.setSizes([210, 1000])

    # ── Statusbar ─────────────────────────────────────────────────────────────

    def _build_statusbar(self):
        sb = self.statusBar()
        self.sb_name  = QLabel("  No archive open")
        self.sb_count = QLabel("")
        self.sb_size  = QLabel("")
        self.sb_dirty = QLabel("")
        self.sb_dirty.setStyleSheet("color: #e06c3a; font-weight: bold;")
        sb.addWidget(self.sb_name)
        sb.addPermanentWidget(self.sb_dirty)
        sb.addPermanentWidget(self.sb_count)
        sb.addPermanentWidget(self.sb_size)

    # ── Archive open/close ────────────────────────────────────────────────────

    def _open_archive(self):
        if self.dirty and not self._confirm_discard(): return
        path, _ = QFileDialog.getOpenFileName(
            self, "Open BA2 Archive", "", "BA2 Archives (*.ba2);;All Files (*)"
        )
        if not path: return
        arc = BA2Archive()
        try:
            arc.open(path)
        except Exception as ex:
            QMessageBox.critical(self, "Error Opening Archive", str(ex)); return
        self._set_archive(arc)

    def _set_archive(self, arc: BA2Archive):
        if self.archive: self.archive.close()
        self.archive = arc
        self.pending_adds.clear(); self.deleted.clear()
        self.replacements.clear(); self.dirty = False
        self._populate_folder_tree()
        self._populate_file_table(arc.files)
        self._update_info()
        self._update_pending_tab()
        self._refresh_statusbar()
        self._refresh_actions()
        self.setWindowTitle(f"BA2 Manager  —  {os.path.basename(arc.path)}")

    def _close_archive(self):
        if self.dirty and not self._confirm_discard(): return
        if self.archive: self.archive.close(); self.archive = None
        self.pending_adds.clear(); self.deleted.clear()
        self.replacements.clear(); self.dirty = False
        self.folder_tree.clear(); self.file_table.setRowCount(0)
        self.info_text.clear(); self.pending_text.clear()
        self._refresh_statusbar(); self._refresh_actions()
        self.setWindowTitle("BA2 Manager")

    def _confirm_discard(self):
        r = QMessageBox.question(
            self, "Unsaved Changes",
            "You have unsaved changes.\nDiscard and continue?",
            QMessageBox.Yes | QMessageBox.No
        )
        return r == QMessageBox.Yes

    # ── Folder tree ───────────────────────────────────────────────────────────

    def _populate_folder_tree(self):
        self.folder_tree.clear()
        if not self.archive: return
        root_item = QTreeWidgetItem(["📦  All Files"])
        root_item.setData(0, Qt.UserRole, None)
        self.folder_tree.addTopLevelItem(root_item)
        dirs: Dict[str, QTreeWidgetItem] = {}
        for e in self.archive.files:
            d = e.directory
            if not d: continue
            parts = d.replace("\\", "/").split("/")
            parent = root_item; path_so_far = ""
            for part in parts:
                path_so_far = (path_so_far + "/" + part).lstrip("/")
                if path_so_far not in dirs:
                    item = QTreeWidgetItem(["📁  " + part])
                    item.setData(0, Qt.UserRole, path_so_far)
                    parent.addChild(item)
                    dirs[path_so_far] = item
                parent = dirs[path_so_far]
        self.folder_tree.expandAll()
        self.folder_tree.setCurrentItem(root_item)

    def _folder_selected(self, current, _prev):
        if not current or not self.archive: return
        folder = current.data(0, Qt.UserRole)
        text   = self.le_filter.text().lower()
        entries = []
        for e in self.archive.files:
            if folder and not e.name.replace("\\", "/").startswith(folder): continue
            if text and text not in e.name.lower(): continue
            entries.append(e)
        self._populate_file_table(entries)

    # ── File table ────────────────────────────────────────────────────────────

    def _populate_file_table(self, entries: List[BA2FileEntry]):
        self.file_table.setSortingEnabled(False)
        self.file_table.setRowCount(0)

        GREEN  = QColor("#2f8c2f")
        RED    = QColor("#c84b2f")
        BLUE   = QColor("#2f7ac8")

        def row_for(e: BA2FileEntry, tag: str = "", color: QColor = None):
            r = self.file_table.rowCount()
            self.file_table.insertRow(r)
            display_name = e.filename + (f"  [{tag}]" if tag else "")
            col0 = QTableWidgetItem(display_name)
            col0.setData(Qt.UserRole, e)
            if color: col0.setForeground(color)
            self.file_table.setItem(r, 0, col0)
            self.file_table.setItem(r, 1, QTableWidgetItem(e.directory))
            sz = QTableWidgetItem(fmt_size(e.unpacked_size))
            sz.setData(Qt.UserRole + 1, e.unpacked_size)
            self.file_table.setItem(r, 2, sz)
            self.file_table.setItem(r, 3, QTableWidgetItem("Yes" if e.is_compressed else "No"))
            self.file_table.setItem(r, 4, QTableWidgetItem(e.format_name))
            dims = f"{e.width} × {e.height}" if e.is_texture and e.width else ""
            self.file_table.setItem(r, 5, QTableWidgetItem(dims))
            mips = str(e.num_mips) if e.is_texture else ""
            self.file_table.setItem(r, 6, QTableWidgetItem(mips))

        for e in entries:
            if e.name in self.deleted:
                row_for(e, "DELETED", RED)
            elif e.name in self.replacements:
                row_for(e, "REPLACED", BLUE)
            else:
                row_for(e)

        # Pending adds
        for iname, src in self.pending_adds.items():
            ne = BA2FileEntry()
            ne.name = iname
            try:
                ne.unpacked_size = os.path.getsize(src)
            except Exception:
                pass
            row_for(ne, "NEW", GREEN)

        self.file_table.setSortingEnabled(True)
        self._refresh_statusbar()

    def _apply_filter(self, text: str):
        if not self.archive: return
        low = text.lower()
        entries = [e for e in self.archive.files if low in e.name.lower()] if low else self.archive.files
        self._populate_file_table(entries)

    def _clear_filter(self):
        self.le_filter.clear()

    def _select_all(self):
        self.file_table.selectAll()

    def _selected_entries(self) -> List[BA2FileEntry]:
        seen = set(); result = []
        for item in self.file_table.selectedItems():
            if item.column() != 0: continue
            e = item.data(Qt.UserRole)
            if e and e.name not in seen:
                seen.add(e.name); result.append(e)
        return result

    # ── Context menu ──────────────────────────────────────────────────────────

    def _context_menu(self, pos):
        menu = QMenu(self)
        menu.addAction("Extract Selected…",  self._extract_selected)
        menu.addAction("Extract All…",       self._extract_all)
        menu.addSeparator()
        menu.addAction("Add Files…",         self._add_files)
        menu.addAction("Replace Selected…",  self._replace_selected)
        menu.addAction("Delete Selected",    self._delete_selected)
        menu.exec_(self.file_table.viewport().mapToGlobal(pos))

    def _on_double_click(self, _):
        self._extract_selected()

    # ── Extract ───────────────────────────────────────────────────────────────

    def _extract_selected(self):
        if not self.archive: return
        entries = self._selected_entries()
        if not entries:
            QMessageBox.information(self, "Nothing Selected", "Select files to extract."); return
        # Filter out deleted/pending-adds (no data to read)
        entries = [e for e in entries if e.name not in self.deleted and e.name not in self.pending_adds]
        self._do_extract(entries)

    def _extract_all(self):
        if not self.archive: return
        self._do_extract(self.archive.files)

    def _worker_busy(self) -> bool:
        return self._worker is not None and self._worker.isRunning()

    def _do_extract(self, entries: List[BA2FileEntry]):
        if self._worker_busy():
            QMessageBox.warning(
                self, "Busy", "Please wait for the current operation to finish before extracting."
            )
            return
        out_dir = QFileDialog.getExistingDirectory(self, "Extract To Folder")
        if not out_dir: return

        prog = WorkProgressDialog("Extracting", self)
        prog.show()

        worker = ExtractWorker(self.archive, entries, out_dir)
        self._worker = worker

        def on_progress(cur, total, name):
            prog.update(cur, total, name)

        def on_finished(ok, msg):
            prog.close()
            if ok: QMessageBox.information(self, "Done", msg)
            else:  QMessageBox.critical(self, "Error", msg)

        worker.progress.connect(on_progress)
        worker.finished.connect(on_finished)
        worker.start()

    # ── New archive ───────────────────────────────────────────────────────────

    def _new_archive(self):
        if self._worker_busy():
            QMessageBox.warning(
                self, "Busy", "Please wait for the current operation to finish before creating an archive."
            )
            return
        dlg = CreateArchiveDialog(self)
        if dlg.exec_() != QDialog.Accepted: return
        s = dlg.get_settings()

        prog = WorkProgressDialog("Creating Archive", self)
        prog.show()

        worker = CreateWorker(s["out_path"], s["source_files"], s["archive_type"],
                              s["root"], s["compress"], s["version"])
        self._worker = worker

        def on_progress(cur, total, name):
            prog.update(cur, total, name)

        def on_finished(ok, msg):
            prog.close()
            if ok:
                r = QMessageBox.question(self, "Archive Created",
                    msg + "\n\nOpen the new archive now?",
                    QMessageBox.Yes | QMessageBox.No)
                if r == QMessageBox.Yes:
                    arc = BA2Archive(); arc.open(s["out_path"]); self._set_archive(arc)
            else:
                QMessageBox.critical(self, "Error", msg)

        worker.progress.connect(on_progress)
        worker.finished.connect(on_finished)
        worker.start()

    # ── Add / Replace / Delete ────────────────────────────────────────────────

    def _add_files(self):
        if not self.archive:
            QMessageBox.information(self, "No Archive", "Open an archive first."); return
        dlg = AddFilesDialog(self.archive.is_texture, self)
        if dlg.exec_() != QDialog.Accepted: return
        new_items = dlg.get_result()
        for iname, src in new_items.items():
            self.pending_adds[iname] = src
        self._mark_dirty()
        self._populate_file_table(self.archive.files)
        self._update_pending_tab()

    def _replace_selected(self):
        if not self.archive: return
        entries = self._selected_entries()
        if len(entries) != 1:
            QMessageBox.information(self, "Select One File",
                "Select exactly one file to replace."); return
        e = entries[0]
        filt = "DDS Textures (*.dds);;All Files (*)" if self.archive.is_texture else "All Files (*)"
        src, _ = QFileDialog.getOpenFileName(
            self, f"Replace  '{e.filename}'  with…", "", filt)
        if not src: return
        self.replacements[e.name] = src
        self._mark_dirty()
        self._populate_file_table(self.archive.files)
        self._update_pending_tab()

    def _delete_selected(self):
        if not self.archive: return
        entries = self._selected_entries()
        if not entries: return
        r = QMessageBox.question(
            self, "Delete Files",
            f"Mark {len(entries)} file(s) for deletion?\n"
            "(Applied when you save the archive.)",
            QMessageBox.Yes | QMessageBox.No)
        if r != QMessageBox.Yes: return
        for e in entries:
            self.deleted.add(e.name)
        self._mark_dirty()
        self._populate_file_table(self.archive.files)
        self._update_pending_tab()

    # ── Save ──────────────────────────────────────────────────────────────────

    def _save_archive(self):
        if not self.archive: return
        if not self.dirty:
            QMessageBox.information(self, "Nothing to Save", "No pending changes."); return
        self._do_rebuild(self.archive.path)

    def _save_archive_as(self):
        if not self.archive: return
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Archive As", self.archive.path,
            "BA2 Archives (*.ba2);;All Files (*)")
        if not path: return
        self._do_rebuild(path)

    def _do_rebuild(self, out_path: str):
        if self._worker_busy():
            QMessageBox.warning(
                self, "Busy", "Please wait for the current operation to finish before saving."
            )
            return
        total = len(self.archive.files) + len(self.pending_adds)
        prog  = WorkProgressDialog("Saving Archive", self)
        prog.show()

        worker = RebuildWorker(
            self.archive, dict(self.pending_adds),
            set(self.deleted), dict(self.replacements), out_path)
        self._worker = worker

        def on_progress(cur, t, name):
            prog.update(cur, t, name)

        def on_finished(ok, msg):
            prog.close()
            if ok:
                arc = BA2Archive(); arc.open(out_path); self._set_archive(arc)
                QMessageBox.information(self, "Saved", msg)
            else:
                QMessageBox.critical(self, "Error", msg)

        worker.progress.connect(on_progress)
        worker.finished.connect(on_finished)
        worker.start()

    # ── Info tabs ─────────────────────────────────────────────────────────────

    def _update_info(self):
        if not self.archive: self.info_text.clear(); return
        arc = self.archive
        lines = [
            f"  Path         {arc.path}",
            f"  Type         {arc.type_name}",
            f"  Version      {_archive_version_label(arc.version)} (header v{arc.version})",
            f"  Files        {len(arc.files):,}",
            f"  Total size   {fmt_size(arc.total_size)}",
            f"  File size    {fmt_size(os.path.getsize(arc.path))}",
            "",
        ]
        if arc.is_texture:
            fmts: Dict[str, int] = {}
            for e in arc.files:
                fmts[e.format_name] = fmts.get(e.format_name, 0) + 1
            lines.append("  Texture Formats:")
            for fn, cnt in sorted(fmts.items(), key=lambda x: -x[1]):
                lines.append(f"    {fn:<34} {cnt:>5} files")
        else:
            comp = sum(1 for e in arc.files if e.is_compressed)
            lines.append(f"  Compressed   {comp:,} / {len(arc.files):,}")
            exts: Dict[str, int] = {}
            for e in arc.files:
                exts[e.ext] = exts.get(e.ext, 0) + 1
            lines.append("")
            lines.append("  Extensions:")
            for ext, cnt in sorted(exts.items(), key=lambda x: -x[1]):
                lines.append(f"    .{ext:<10} {cnt:>5} files")
        self.info_text.setPlainText("\n".join(lines))

    def _update_pending_tab(self):
        lines = []
        if self.pending_adds:
            lines.append(f"PENDING ADDITIONS ({len(self.pending_adds)})")
            lines.append("─" * 60)
            for iname, src in self.pending_adds.items():
                lines.append(f"  ADD  {iname}")
                lines.append(f"       ← {src}")
            lines.append("")
        if self.deleted:
            lines.append(f"PENDING DELETIONS ({len(self.deleted)})")
            lines.append("─" * 60)
            for n in sorted(self.deleted):
                lines.append(f"  DEL  {n}")
            lines.append("")
        if self.replacements:
            lines.append(f"PENDING REPLACEMENTS ({len(self.replacements)})")
            lines.append("─" * 60)
            for iname, src in self.replacements.items():
                lines.append(f"  REP  {iname}")
                lines.append(f"       ← {src}")
            lines.append("")
        if not lines:
            lines = ["No pending changes."]
        self.pending_text.setPlainText("\n".join(lines))
        total_pending = len(self.pending_adds) + len(self.deleted) + len(self.replacements)
        self.tabs.setTabText(2, f"Pending Changes ({total_pending})")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _mark_dirty(self):
        self.dirty = True
        self._refresh_statusbar()
        self._refresh_actions()

    def _refresh_statusbar(self):
        if self.archive:
            self.sb_name.setText(f"  {os.path.basename(self.archive.path)}  [{self.archive.type_name}]")
            n = self.file_table.rowCount()
            self.sb_count.setText(f"  {n:,} files  ")
            self.sb_size.setText(f"  {fmt_size(self.archive.total_size)}  ")
        else:
            self.sb_name.setText("  No archive open")
            self.sb_count.setText("")
            self.sb_size.setText("")
        self.sb_dirty.setText("  ● UNSAVED CHANGES  " if self.dirty else "")

    def _refresh_actions(self):
        has = self.archive is not None
        for a in [self.act_close, self.act_save, self.act_save_as,
                  self.act_add, self.act_rep, self.act_del, self.act_selall,
                  self.act_ext_sel, self.act_ext_all,
                  self.tb_save, self.tb_ext_sel, self.tb_ext_all,
                  self.tb_add, self.tb_rep, self.tb_del]:
            a.setEnabled(has)

    def _about(self):
        QMessageBox.about(self, "About BA2 Manager",
            "<b>BA2 Manager</b><br>"
            "Version 1.0<br><br>"
            "A combined BA2 archive browser, extractor, creator and editor.<br>"
            "Merges the functionality of BSA Browser and BAMgr.<br><br>"
            "<b>Supported formats:</b><br>"
            "• General (GNRL) — meshes, sounds, scripts, strings<br>"
            "• DX10 — Textures (.dds with DDS header reconstruction)<br><br>"
            "<b>Games:</b> Fallout 4 / Fallout 76 (BA2 v1, v7, v8), Starfield (BA2 v2–v3)<br><br>"
            "Built with Python 3 + PyQt5.<br>"
            "Run with:  <code>python ba2_manager.py [file.ba2]</code>"
        )

    def closeEvent(self, event):
        if self.dirty and not self._confirm_discard():
            event.ignore(); return
        if self.archive: self.archive.close()
        event.accept()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("BA2 Manager")
    app.setStyle("Fusion")

    # Dark theme
    p = QPalette()
    dark   = QColor(28, 28, 28)
    mid    = QColor(44, 44, 44)
    light  = QColor(58, 58, 58)
    text   = QColor(218, 218, 218)
    accent = QColor(58, 116, 196)
    p.setColor(QPalette.Window,          dark)
    p.setColor(QPalette.WindowText,      text)
    p.setColor(QPalette.Base,            QColor(18, 18, 18))
    p.setColor(QPalette.AlternateBase,   mid)
    p.setColor(QPalette.ToolTipBase,     mid)
    p.setColor(QPalette.ToolTipText,     text)
    p.setColor(QPalette.Text,            text)
    p.setColor(QPalette.Button,          mid)
    p.setColor(QPalette.ButtonText,      text)
    p.setColor(QPalette.BrightText,      Qt.red)
    p.setColor(QPalette.Link,            accent)
    p.setColor(QPalette.Highlight,       accent)
    p.setColor(QPalette.HighlightedText, Qt.white)
    p.setColor(QPalette.Disabled, QPalette.Text, QColor(95, 95, 95))
    p.setColor(QPalette.Disabled, QPalette.ButtonText, QColor(95, 95, 95))
    app.setPalette(p)

    app.setStyleSheet("""
        QMainWindow, QDialog { background: #1c1c1c; }
        QGroupBox {
            border: 1px solid #404040;
            border-radius: 5px;
            margin-top: 10px;
            padding-top: 10px;
            font-weight: bold;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 4px;
        }
        QTableWidget {
            gridline-color: #2e2e2e;
            border: 1px solid #404040;
        }
        QHeaderView::section {
            background: #2c2c2c;
            border: none;
            border-right: 1px solid #404040;
            border-bottom: 1px solid #404040;
            padding: 4px 8px;
            font-weight: bold;
        }
        QTreeWidget {
            border: 1px solid #404040;
        }
        QTabWidget::pane {
            border: 1px solid #404040;
        }
        QTabBar::tab {
            padding: 6px 16px;
            background: #2c2c2c;
            border: 1px solid #404040;
            border-bottom: none;
            margin-right: 2px;
        }
        QTabBar::tab:selected {
            background: #1c1c1c;
            border-bottom: 2px solid #3a74c4;
        }
        QLineEdit, QComboBox, QSpinBox, QTextEdit {
            background: #141414;
            border: 1px solid #505050;
            border-radius: 3px;
            padding: 4px 6px;
            selection-background-color: #3a74c4;
        }
        QPushButton {
            background: #2c2c2c;
            border: 1px solid #505050;
            border-radius: 3px;
            padding: 5px 14px;
            min-width: 60px;
        }
        QPushButton:hover { background: #383838; border-color: #707070; }
        QPushButton:pressed { background: #1e1e1e; }
        QPushButton:disabled { color: #606060; border-color: #383838; }
        QToolBar {
            background: #242424;
            border-bottom: 1px solid #404040;
            spacing: 4px;
            padding: 3px;
        }
        QToolBar QToolButton {
            padding: 4px 10px;
            background: transparent;
            border: 1px solid transparent;
            border-radius: 3px;
        }
        QToolBar QToolButton:hover { background: #383838; border-color: #505050; }
        QMenuBar { background: #242424; }
        QMenuBar::item:selected { background: #383838; }
        QMenu { background: #2c2c2c; border: 1px solid #505050; }
        QMenu::item:selected { background: #3a74c4; }
        QProgressBar {
            border: 1px solid #505050;
            border-radius: 3px;
            text-align: center;
            height: 18px;
        }
        QProgressBar::chunk { background: #3a74c4; border-radius: 2px; }
        QStatusBar { background: #242424; border-top: 1px solid #404040; }
        QScrollBar:vertical { width: 12px; background: #1c1c1c; }
        QScrollBar::handle:vertical { background: #484848; border-radius: 5px; min-height: 20px; }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
        QScrollBar:horizontal { height: 12px; background: #1c1c1c; }
        QScrollBar::handle:horizontal { background: #484848; border-radius: 5px; min-width: 20px; }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }
    """)

    win = BA2Manager()
    win.show()

    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        try:
            arc = BA2Archive()
            arc.open(sys.argv[1])
            win._set_archive(arc)
        except Exception as ex:
            QMessageBox.critical(win, "Error", str(ex))

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
