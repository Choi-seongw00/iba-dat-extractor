#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import datetime as dt
from pathlib import Path
from struct import unpack

import pyarrow as pa
import pyarrow.parquet as pq


def decadr(value: int) -> int:
    b0, b1, b2, b3, b4, b5, b6, b7 = value.to_bytes(8, "little")
    a0, a1, a2 = b6 ^ b0, b5 ^ b4 ^ b3, b7 ^ b1
    return a0 | (a1 << 8) | (a2 << 16)


def read_tags(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    rhs = text.split("=", 1)[1].strip() if "=" in text else text.strip()

    try:
        parsed = ast.literal_eval(rhs)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass

    return [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]


def decode_meta_text(data: bytes) -> str:
    for encoding in ("cp949", "euc-kr", "utf-8", "latin-1"):
        try:
            return data.decode(encoding, errors="replace")
        except Exception:
            continue
    return data.decode("latin-1", errors="replace")


def parse_metadata(raw: bytes) -> tuple[dict[str, str], list[dict[str, str]]]:
    header_ptr = int.from_bytes(raw[8:12], "little")
    if header_ptr <= 0 or header_ptr >= len(raw):
        header_ptr = 36

    end_ascii_pos = raw.find(b"endASCII:\r\n", header_ptr)
    marker_len = len(b"endASCII:\r\n")
    if end_ascii_pos < 0:
        end_ascii_pos = raw.find(b"endASCII:\n", header_ptr)
        marker_len = len(b"endASCII:\n")
    if end_ascii_pos < 0:
        raise ValueError("endASCII 마커를 찾을 수 없습니다.")

    meta_text = decode_meta_text(raw[header_ptr : end_ascii_pos + marker_len])

    global_meta: dict[str, str] = {}
    channels: list[dict[str, str]] = []
    current_channel: dict[str, str] | None = None

    for raw_line in meta_text.replace("\r\n", "\n").split("\n"):
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("beginchannel:"):
            current_channel = {"channel_index": line.split(":", 1)[1].strip()}
            continue
        if line == "endchannel:":
            if current_channel is not None:
                channels.append(current_channel)
            current_channel = None
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if current_channel is not None:
            current_channel[key] = value
        else:
            global_meta[key] = value

    return global_meta, channels


def parse_start_time(text: str) -> dt.datetime:
    for fmt in ("%d.%m.%Y %H:%M:%S.%f", "%d.%m.%Y %H:%M:%S"):
        try:
            return dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"starttime 포맷을 해석할 수 없습니다: {text}")


def _to_float(value: str | None, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def decode_channel_samples(
    raw: bytes,
    channel: dict[str, str],
    target_frames: list[int],
    master_clk: float = 0.001,
) -> list[float | None]:
    offset_text = channel.get("channel_offset", "")
    if not offset_text.startswith("O"):
        return [None] * len(target_frames)

    ptr = decadr(int(offset_text[1:], 16))
    is_digital = "digchannel" in channel
    pda_type = str(channel.get("$PDA_Typ", "")).lower()

    channel_tbase = _to_float(channel.get("$PDA_Tbase"), master_clk)
    if channel_tbase <= 0:
        channel_tbase = master_clk

    ratio = channel_tbase / master_clk if master_clk > 0 else 1.0
    if ratio <= 0:
        ratio = 1.0

    target_channel_frames = [int(frame / ratio) for frame in target_frames]

    minscale = _to_float(channel.get("minscale"), 0.0)
    maxscale = _to_float(channel.get("maxscale"), 0.0)

    def read_analog_value(cursor: int) -> tuple[float | None, int]:
        if pda_type == "int16":
            if cursor + 2 > len(raw):
                return None, cursor
            raw_value = int.from_bytes(raw[cursor : cursor + 2], "little", signed=True)
            scale = (maxscale - minscale) / 65535.0
            value = minscale + (raw_value + 32768) * scale
            return value, cursor + 2

        if pda_type == "uint16":
            if cursor + 2 > len(raw):
                return None, cursor
            raw_value = int.from_bytes(raw[cursor : cursor + 2], "little", signed=False)
            scale = (maxscale - minscale) / 65535.0
            value = minscale + raw_value * scale
            return value, cursor + 2

        if pda_type == "int32":
            if cursor + 4 > len(raw):
                return None, cursor
            raw_value = int.from_bytes(raw[cursor : cursor + 4], "little", signed=True)
            return float(raw_value), cursor + 4

        if pda_type == "uint32":
            if cursor + 4 > len(raw):
                return None, cursor
            raw_value = int.from_bytes(raw[cursor : cursor + 4], "little", signed=False)
            return float(raw_value), cursor + 4

        if cursor + 4 > len(raw):
            return None, cursor
        value = float(unpack("<f", raw[cursor : cursor + 4])[0])
        return value, cursor + 4

    samples: list[float | None] = [None] * len(target_frames)
    target_index = 0
    current_frame = 0
    visited_ptrs: set[int] = set()

    while ptr and target_index < len(target_frames):
        if ptr in visited_ptrs or ptr + 6 > len(raw):
            break
        visited_ptrs.add(ptr)

        run_count = int.from_bytes(raw[ptr : ptr + 2], "little")
        next_ptr = int.from_bytes(raw[ptr + 2 : ptr + 6], "little")
        cursor = ptr + 6

        for _ in range(run_count):
            if cursor >= len(raw):
                break

            repeat = raw[cursor]
            cursor += 1

            if is_digital:
                if cursor >= len(raw):
                    break
                value = 1.0 if raw[cursor] == 0xC0 else 0.0
                cursor += 1
            else:
                value, cursor_after = read_analog_value(cursor)
                if value is None:
                    break
                cursor = cursor_after

            run_start = current_frame
            run_end = current_frame + repeat - 1

            while target_index < len(target_channel_frames) and target_channel_frames[target_index] <= run_end:
                if target_channel_frames[target_index] >= run_start:
                    samples[target_index] = value
                target_index += 1

            current_frame += repeat

        ptr = next_ptr

    return samples


def main() -> None:
    parser = argparse.ArgumentParser(description="DAT에서 태그 시계열을 1초 단위(또는 지정 간격)로 Parquet 추출")
    parser.add_argument("dat_file", type=Path, help="입력 DAT 파일")
    parser.add_argument("--tag-file", type=Path, required=True, help="태그 파일(tags.md 또는 줄단위 txt)")
    parser.add_argument("--step", type=int, default=1000, help="프레임 간격 (기본 1000=1초)")
    parser.add_argument("--output", type=Path, default=Path("tags_1s.parquet"), help="출력 Parquet 파일")
    args = parser.parse_args()

    raw = args.dat_file.read_bytes()
    global_meta, channels = parse_metadata(raw)
    channel_map = {channel.get("name", ""): channel for channel in channels if channel.get("name")}

    tags = read_tags(args.tag_file)
    start_time = parse_start_time(global_meta["starttime"])
    clk = float(global_meta["clk"])
    frames = int(global_meta["frames"])

    step = max(1, args.step)
    target_frames = list(range(0, frames, step))
    times = [start_time + dt.timedelta(seconds=frame * clk) for frame in target_frames]

    columns: dict[str, list[float | None] | list[dt.datetime]] = {"time": times}
    missing: list[str] = []

    for tag in tags:
        channel = channel_map.get(tag)
        if channel is None:
            missing.append(tag)
            continue
        if str(channel.get("$PDA_Typ", "")).lower() == "text":
            missing.append(tag)
            continue
        columns[tag] = decode_channel_samples(raw, channel, target_frames, master_clk=clk)

    table = pa.table(columns)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.output, compression="zstd")

    print(f"[저장 완료] {args.output}")
    print(f"[행 수] {table.num_rows}")
    print(f"[컬럼 수] {table.num_columns} (time 포함)")
    print(f"[요청 태그] {len(tags)} / [저장 태그] {table.num_columns - 1}")
    print(f"[누락/스킵 태그] {len(missing)}")
    if missing:
        print("[누락 예시]", missing[:5])


if __name__ == "__main__":
    main()
