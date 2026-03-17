#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import csv
import datetime as dt
import re
from pathlib import Path
from struct import unpack
from typing import Any


def decadr(value: int) -> int:
    b0, b1, b2, b3, b4, b5, b6, b7 = value.to_bytes(8, "little")
    a0, a1, a2 = b6 ^ b0, b5 ^ b4 ^ b3, b7 ^ b1
    return a0 | (a1 << 8) | (a2 << 16)


def _read_tag_file(path: Path | None) -> list[str]:
    if path is None:
        return []
    text = path.read_text(encoding="utf-8", errors="replace")

    rhs = text.split("=", 1)[1].strip() if "=" in text else text.strip()
    try:
        parsed = ast.literal_eval(rhs)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass

    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _compile_patterns(regex_list: list[str]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for pattern in regex_list:
        try:
            patterns.append(re.compile(pattern))
        except re.error as error:
            raise ValueError(f"잘못된 정규식: {pattern} ({error})") from error
    return patterns


def _select_tags(
    all_names: list[str],
    exact_tags: list[str],
    contains_tags: list[str],
    regex_patterns: list[re.Pattern[str]],
) -> list[str]:
    selected: list[str] = []
    selected_set: set[str] = set()

    def add(name: str) -> None:
        if name not in selected_set:
            selected_set.add(name)
            selected.append(name)

    for tag in exact_tags:
        for name in all_names:
            if name == tag:
                add(name)

    for needle in contains_tags:
        for name in all_names:
            if needle in name:
                add(name)

    for pattern in regex_patterns:
        for name in all_names:
            if pattern.search(name):
                add(name)

    return selected


def _decode_metadata_text(metadata_bytes: bytes, metadata_encoding: str) -> tuple[str, str]:
    if metadata_encoding != "auto":
        return metadata_bytes.decode(metadata_encoding, errors="replace"), metadata_encoding

    candidates = ["cp949", "euc-kr", "utf-8", "latin-1"]
    marker_tokens = [
        "beginheader:",
        "endheader:",
        "beginmodule:",
        "beginchannel:",
        "endASCII:",
    ]

    best_text = metadata_bytes.decode("latin-1", errors="replace")
    best_encoding = "latin-1"
    best_score = float("-inf")

    for encoding in candidates:
        had_decode_error = False
        try:
            text = metadata_bytes.decode(encoding, errors="strict")
        except UnicodeDecodeError:
            text = metadata_bytes.decode(encoding, errors="replace")
            had_decode_error = True

        marker_score = sum(token in text for token in marker_tokens) * 100
        hangul_score = sum("\uac00" <= char <= "\ud7a3" for char in text) * 3
        replacement_penalty = text.count("\ufffd") * 20
        decode_error_penalty = 200 if had_decode_error else 0
        latin1_penalty = 20 if encoding == "latin-1" else 0
        score = (
            marker_score
            + hangul_score
            - replacement_penalty
            - decode_error_penalty
            - latin1_penalty
        )

        if score > best_score:
            best_score = score
            best_text = text
            best_encoding = encoding

    return best_text, best_encoding


def _parse_metadata(raw: bytes, metadata_encoding: str) -> tuple[dict[str, Any], list[dict[str, str]], str]:
    if len(raw) < 12:
        raise ValueError("파일이 너무 짧아서 PDA 헤더를 읽을 수 없습니다.")

    header_ptr = int.from_bytes(raw[8:12], "little")
    if header_ptr <= 0 or header_ptr >= len(raw):
        raise ValueError("헤더 포인터 값이 비정상입니다.")

    end_ascii_marker = b"endASCII:\r\n"
    end_ascii_pos = raw.find(end_ascii_marker, header_ptr)
    marker_len = len(end_ascii_marker)

    if end_ascii_pos < 0:
        end_ascii_marker = b"endASCII:\n"
        end_ascii_pos = raw.find(end_ascii_marker, header_ptr)
        marker_len = len(end_ascii_marker)
    if end_ascii_pos < 0:
        raise ValueError("'endASCII:' 마커를 찾을 수 없습니다.")

    ascii_bytes = raw[header_ptr : end_ascii_pos + marker_len]
    text, used_encoding = _decode_metadata_text(ascii_bytes, metadata_encoding)

    global_meta: dict[str, Any] = {}
    channels: list[dict[str, str]] = []
    current_channel: dict[str, str] | None = None

    for raw_line in text.replace("\r\n", "\n").split("\n"):
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
            if key not in global_meta:
                global_meta[key] = value
            else:
                prev = global_meta[key]
                if isinstance(prev, list):
                    prev.append(value)
                else:
                    global_meta[key] = [prev, value]

    return global_meta, channels, used_encoding


def _decode_channel_values(raw: bytes, channel: dict[str, str]) -> list[float | bool]:
    offset_text = channel.get("channel_offset", "")
    if not offset_text.startswith("O"):
        raise ValueError("channel_offset 형식이 올바르지 않습니다.")

    ptr = decadr(int(offset_text[1:], 16))
    is_digital = "digchannel" in channel

    values: list[float | bool] = []
    visited: set[int] = set()

    while ptr:
        if ptr in visited:
            break
        visited.add(ptr)

        if ptr + 6 > len(raw):
            break

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
                value = raw[cursor] == 0xC0
                cursor += 1
            else:
                if cursor + 4 > len(raw):
                    break
                value = unpack("<f", raw[cursor : cursor + 4])[0]
                cursor += 4

            values.extend([value] * repeat)

        ptr = next_ptr

    return values


def _parse_start_time(text: str) -> dt.datetime:
    for fmt in ("%d.%m.%Y %H:%M:%S.%f", "%d.%m.%Y %H:%M:%S"):
        try:
            return dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"starttime 포맷을 해석할 수 없습니다: {text}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PDA2 .dat 파일에서 태그명을 필터링해 시계열 데이터를 CSV로 추출합니다. (macOS 지원)"
    )
    parser.add_argument("dat_file", type=Path, help="입력 .dat 파일 경로")
    parser.add_argument(
        "--metadata-encoding",
        choices=["auto", "utf-8", "euc-kr", "cp949", "latin-1"],
        default="auto",
        help="메타데이터 해석 인코딩 (기본: auto)",
    )
    parser.add_argument("--tag", action="append", default=[], help="정확 일치 태그명")
    parser.add_argument("--tag-file", type=Path, help="태그명 목록 파일 (UTF-8, 한 줄당 1개)")
    parser.add_argument("--contains", action="append", default=[], help="부분 문자열 필터")
    parser.add_argument("--regex", action="append", default=[], help="정규식 필터")
    parser.add_argument("--list-only", action="store_true", help="매칭 태그 목록만 출력")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("selected_tag_timeseries.csv"),
        help="출력 CSV 경로",
    )
    parser.add_argument("--start-frame", type=int, default=0, help="시작 프레임")
    parser.add_argument("--end-frame", type=int, default=-1, help="종료 프레임(-1: 끝까지)")
    parser.add_argument("--step", type=int, default=1, help="샘플링 간격")
    args = parser.parse_args()

    raw = args.dat_file.read_bytes()
    global_meta, channels, used_encoding = _parse_metadata(raw, args.metadata_encoding)

    all_names = [channel.get("name", "") for channel in channels if channel.get("name")]
    tag_list = list(args.tag) + _read_tag_file(args.tag_file)
    patterns = _compile_patterns(list(args.regex))

    if not tag_list and not args.contains and not patterns:
        raise ValueError("최소 1개 이상의 필터(--tag, --tag-file, --contains, --regex)가 필요합니다.")

    selected = _select_tags(all_names, tag_list, list(args.contains), patterns)

    print(f"[메타 인코딩] {used_encoding}")
    print(f"[전체 채널] {len(all_names)}")
    print(f"[선택 채널] {len(selected)}")

    if not selected:
        print("조건과 일치하는 태그가 없습니다.")
        return

    selected_map = {channel.get("name", ""): channel for channel in channels if channel.get("name") in selected}

    print("[선택 태그]")
    for name in selected:
        print(f"- {name}")

    if args.list_only:
        return

    start_time = _parse_start_time(str(global_meta.get("starttime", "")))
    clk = float(global_meta.get("clk", "0"))
    frames = int(global_meta.get("frames", "0"))

    decoded_values: dict[str, list[float | bool]] = {}
    for name in selected:
        channel = selected_map[name]
        typ = str(channel.get("$PDA_Typ", "")).lower()
        if typ == "text":
            print(f"[건너뜀] text 타입 태그는 현재 미지원: {name}")
            continue
        try:
            decoded_values[name] = _decode_channel_values(raw, channel)
        except Exception as error:
            print(f"[실패] {name}: {error}")

    if not decoded_values:
        print("추출 가능한 태그가 없습니다.")
        return

    max_len = max(len(values) for values in decoded_values.values())
    effective_frames = frames if frames > 0 else max_len
    start = max(0, args.start_frame)
    stop = effective_frames if args.end_frame < 0 else min(effective_frames, args.end_frame + 1)
    step = max(1, args.step)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        tag_order = [name for name in selected if name in decoded_values]
        writer.writerow(["frame", "time", *tag_order])

        for frame in range(start, stop, step):
            timestamp = start_time + dt.timedelta(seconds=frame * clk)
            row: list[Any] = [frame, timestamp.isoformat(sep=" ")]
            for name in tag_order:
                values = decoded_values[name]
                row.append(values[frame] if frame < len(values) else "")
            writer.writerow(row)

    print(f"[저장 완료] {args.output}")
    print(
        f"[범위] start={start}, end={stop - 1 if stop > start else start}, step={step}, rows={len(range(start, stop, step))}"
    )


if __name__ == "__main__":
    main()
