#!/usr/bin/env python3
"""Build a portable Windows executable wrapper around the Baluffo ship bundle."""

from __future__ import annotations

import argparse
import binascii
import math
import shutil
import struct
import subprocess
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "baluffo-portable"
DEFAULT_EXE_NAME = "Baluffo"
DEFAULT_ICON_SIZE = 256
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_ship_bundle import DEFAULT_BUNDLE_VERSION, build_bundle


def _copy_tree_contents(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def _mix_channel(a: int, b: int, t: float) -> int:
    t = 0.0 if t < 0.0 else 1.0 if t > 1.0 else t
    return int(round((a * (1.0 - t)) + (b * t)))


def _mix_rgb(left: tuple[int, int, int], right: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    return (
        _mix_channel(left[0], right[0], t),
        _mix_channel(left[1], right[1], t),
        _mix_channel(left[2], right[2], t),
    )


def _point_in_rounded_rect(x: float, y: float, *, size: int, radius: float) -> bool:
    left = radius
    right = size - radius
    top = radius
    bottom = size - radius
    if left <= x <= right or top <= y <= bottom:
        return True
    corners = (
        (left, top),
        (right, top),
        (left, bottom),
        (right, bottom),
    )
    for cx, cy in corners:
        if (x - cx) ** 2 + (y - cy) ** 2 <= radius**2:
            return True
    return False


def _point_in_ellipse(x: float, y: float, *, cx: float, cy: float, rx: float, ry: float) -> bool:
    return (((x - cx) / rx) ** 2 + ((y - cy) / ry) ** 2) <= 1.0


def _point_in_rounded_box(x: float, y: float, *, left: float, top: float, right: float, bottom: float, radius: float) -> bool:
    if left + radius <= x <= right - radius and top <= y <= bottom:
        return True
    if left <= x <= right and top + radius <= y <= bottom - radius:
        return True
    corners = (
        (left + radius, top + radius),
        (right - radius, top + radius),
        (left + radius, bottom - radius),
        (right - radius, bottom - radius),
    )
    for cx, cy in corners:
        if (x - cx) ** 2 + (y - cy) ** 2 <= radius**2:
            return True
    return False


def _distance_to_segment(
    px: float,
    py: float,
    ax: float,
    ay: float,
    bx: float,
    by: float,
) -> float:
    abx = bx - ax
    aby = by - ay
    length_sq = (abx * abx) + (aby * aby)
    if length_sq <= 0.0:
        return math.hypot(px - ax, py - ay)
    t = ((px - ax) * abx + (py - ay) * aby) / length_sq
    t = 0.0 if t < 0.0 else 1.0 if t > 1.0 else t
    nearest_x = ax + (abx * t)
    nearest_y = ay + (aby * t)
    return math.hypot(px - nearest_x, py - nearest_y)


def _point_in_stroke_segment(
    px: float,
    py: float,
    ax: float,
    ay: float,
    bx: float,
    by: float,
    width: float,
) -> bool:
    return _distance_to_segment(px, py, ax, ay, bx, by) <= (width * 0.5)


def _point_in_stroke_circle(px: float, py: float, *, cx: float, cy: float, radius: float, width: float) -> bool:
    distance = math.hypot(px - cx, py - cy)
    half = width * 0.5
    return (radius - half) <= distance <= (radius + half)


def _encode_png_rgba(size: int, rgba_rows: list[bytes]) -> bytes:
    def chunk(name: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + name
            + data
            + struct.pack(">I", binascii.crc32(name + data) & 0xFFFFFFFF)
        )

    raw = b"".join(b"\x00" + row for row in rgba_rows)
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    return b"".join(
        [
            b"\x89PNG\r\n\x1a\n",
            chunk(b"IHDR", ihdr),
            chunk(b"IDAT", zlib.compress(raw, level=9)),
            chunk(b"IEND", b""),
        ]
    )


def _render_icon_png(size: int = DEFAULT_ICON_SIZE) -> bytes:
    background_start = (103, 191, 255)
    background_end = (209, 69, 255)
    glow = (255, 255, 255)
    outline = (255, 248, 236)
    shadow = (73, 94, 196)
    radius = size * 0.24
    rows: list[bytes] = []
    doc_left = size * 0.29
    doc_top = size * 0.18
    doc_right = size * 0.63
    doc_bottom = size * 0.72
    doc_radius = size * 0.06
    stroke = max(3.0, size * 0.028)
    small_stroke = max(2.0, size * 0.024)

    for y in range(size):
        row = bytearray()
        for x in range(size):
            px = x + 0.5
            py = y + 0.5
            if not _point_in_rounded_rect(px, py, size=size, radius=radius):
                row.extend((0, 0, 0, 0))
                continue

            diagonal_t = ((px / size) * 0.4) + ((py / size) * 0.6)
            color = list(_mix_rgb(background_start, background_end, diagonal_t))

            glow_dx = px - (size * 0.28)
            glow_dy = py - (size * 0.18)
            glow_dist = math.hypot(glow_dx, glow_dy) / (size * 0.62)
            glow_strength = max(0.0, 1.0 - glow_dist)
            for index, channel in enumerate(glow):
                color[index] = _mix_channel(color[index], channel, glow_strength * 0.32)

            vignette = abs((px / size) - 0.5) + abs((py / size) - 0.5)
            for index in range(3):
                color[index] = _mix_channel(color[index], shadow[index], max(0.0, (vignette - 0.42) * 0.30))

            is_document = False
            if _point_in_rounded_box(
                px,
                py,
                left=doc_left,
                top=doc_top,
                right=doc_right,
                bottom=doc_bottom,
                radius=doc_radius,
            ) and not _point_in_rounded_box(
                px,
                py,
                left=doc_left + stroke,
                top=doc_top + stroke,
                right=doc_right - stroke,
                bottom=doc_bottom - stroke,
                radius=max(1.0, doc_radius - stroke),
            ):
                is_document = True

            folded_edge = _point_in_stroke_segment(
                px,
                py,
                doc_right - (size * 0.10),
                doc_top + stroke,
                doc_right - stroke,
                doc_top + (size * 0.10),
                stroke,
            ) or _point_in_stroke_segment(
                px,
                py,
                doc_right - (size * 0.10),
                doc_top + stroke,
                doc_right - (size * 0.10),
                doc_top + (size * 0.10),
                stroke,
            ) or _point_in_stroke_segment(
                px,
                py,
                doc_right - (size * 0.10),
                doc_top + (size * 0.10),
                doc_right - stroke,
                doc_top + (size * 0.10),
                stroke,
            )

            line_one = _point_in_stroke_segment(px, py, size * 0.36, size * 0.31, size * 0.54, size * 0.31, small_stroke)
            line_two = _point_in_stroke_segment(px, py, size * 0.36, size * 0.40, size * 0.53, size * 0.40, small_stroke)
            line_three = _point_in_stroke_segment(px, py, size * 0.36, size * 0.49, size * 0.49, size * 0.49, small_stroke)
            bullet_one = math.hypot(px - (size * 0.33), py - (size * 0.40)) <= (small_stroke * 0.55)
            bullet_two = math.hypot(px - (size * 0.33), py - (size * 0.49)) <= (small_stroke * 0.55)

            finger = _point_in_stroke_segment(px, py, size * 0.60, size * 0.67, size * 0.60, size * 0.50, stroke)
            hand_left = _point_in_stroke_segment(px, py, size * 0.60, size * 0.64, size * 0.52, size * 0.57, stroke)
            hand_right = _point_in_stroke_segment(px, py, size * 0.60, size * 0.62, size * 0.68, size * 0.57, stroke)
            wrist = _point_in_stroke_segment(px, py, size * 0.55, size * 0.71, size * 0.66, size * 0.71, stroke)
            tap_ring = _point_in_stroke_circle(
                px,
                py,
                cx=size * 0.60,
                cy=size * 0.49,
                radius=size * 0.058,
                width=small_stroke,
            )

            if is_document or folded_edge or line_one or line_two or line_three or bullet_one or bullet_two or finger or hand_left or hand_right or wrist or tap_ring:
                color = [*outline]

            row.extend((color[0], color[1], color[2], 255))
        rows.append(bytes(row))
    return _encode_png_rgba(size, rows)


def generate_icon_file(output_path: Path, *, size: int = DEFAULT_ICON_SIZE) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    png_payload = _render_icon_png(size=size)
    icon_dir = struct.pack("<HHH", 0, 1, 1)
    entry = struct.pack("<BBBBHHII", 0, 0, 0, 0, 1, 32, len(png_payload), 6 + 16)
    output_path.write_bytes(icon_dir + entry + png_payload)
    return output_path


def resolve_icon_path(output_dir: Path, *, exe_name: str, icon_arg: str = "") -> Path:
    if str(icon_arg or "").strip():
        icon_path = Path(icon_arg).expanduser().resolve()
        if not icon_path.exists():
            raise RuntimeError(f"Icon file not found: {icon_path}")
        return icon_path
    generated_dir = output_dir.parent / ".pyinstaller-assets"
    return generate_icon_file(generated_dir / f"{exe_name}.ico")


def build_portable_layout(output_dir: Path, version: str) -> Path:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    build_bundle(output_dir / "ship", version)
    return output_dir


def run_pyinstaller(output_dir: Path, *, exe_name: str, icon_path: Path) -> Path:
    pyinstaller_dist = output_dir.parent / ".pyinstaller-dist"
    pyinstaller_work = output_dir.parent / ".pyinstaller-work"
    pyinstaller_spec = output_dir.parent / ".pyinstaller-spec"
    for path in (pyinstaller_dist, pyinstaller_work, pyinstaller_spec):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--windowed",
        "--name",
        exe_name,
        "--icon",
        str(icon_path),
        "--distpath",
        str(pyinstaller_dist),
        "--workpath",
        str(pyinstaller_work),
        "--specpath",
        str(pyinstaller_spec),
        str(ROOT / "scripts" / "ship" / "desktop_app.py"),
    ]
    subprocess.run(command, check=True, cwd=str(ROOT))
    built_dir = pyinstaller_dist / exe_name
    if not built_dir.exists():
        raise RuntimeError(f"PyInstaller output not found: {built_dir}")
    _copy_tree_contents(built_dir, output_dir)
    exe_path = output_dir / f"{exe_name}.exe"
    if not exe_path.exists():
        raise RuntimeError(f"Portable executable not found: {exe_path}")
    return exe_path


def create_zip(output_dir: Path, *, version: str) -> Path:
    archive_base = output_dir.parent / f"{output_dir.name}-{version}"
    archive_path = archive_base.with_suffix(".zip")
    if archive_path.exists():
        archive_path.unlink()
    built = shutil.make_archive(str(archive_base), "zip", root_dir=str(output_dir))
    return Path(built)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dist/baluffo-portable executable wrapper.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--bundle-version", default=DEFAULT_BUNDLE_VERSION)
    parser.add_argument("--exe-name", default=DEFAULT_EXE_NAME)
    parser.add_argument("--icon", default="")
    parser.add_argument("--skip-zip", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    version = str(args.bundle_version).strip() or DEFAULT_BUNDLE_VERSION
    portable_root = build_portable_layout(output_dir, version)
    exe_name = str(args.exe_name).strip() or DEFAULT_EXE_NAME
    icon_path = resolve_icon_path(portable_root, exe_name=exe_name, icon_arg=str(args.icon or ""))
    exe_path = run_pyinstaller(portable_root, exe_name=exe_name, icon_path=icon_path)
    print(f"Portable executable ready: {exe_path}")
    print(f"Ship bundle root: {portable_root / 'ship'}")
    print(f"Executable icon: {icon_path}")
    if not args.skip_zip:
        archive = create_zip(portable_root, version=version)
        print(f"Portable archive: {archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
