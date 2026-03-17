#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import mmap
from pathlib import Path
from typing import Any


def _split_key_value(line: str) -> tuple[str, str] | None:
    if not line or ":" not in line:
        return None
    key, value = line.split(":", 1)
    return key.strip(), value.strip()


def _append_value(target: dict[str, Any], key: str, value: str) -> None:
    if key not in target:
        target[key] = value
        return
    current = target[key]
    if isinstance(current, list):
        current.append(value)
    else:
        target[key] = [current, value]


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


def parse_pda_dat(dat_path: Path, metadata_encoding: str = "auto") -> dict[str, Any]:
    with dat_path.open("rb") as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            file_size = mm.size()
            magic = mm[:4].decode("ascii", errors="replace")

            begin_header_offset = mm.find(b"beginheader:")
            if begin_header_offset < 0:
                raise ValueError("'beginheader:' 마커를 찾을 수 없습니다.")

            end_ascii_offset = mm.find(b"endASCII:", begin_header_offset)
            if end_ascii_offset < 0:
                raise ValueError("'endASCII:' 마커를 찾을 수 없습니다.")

            line_end_offset = mm.find(b"\n", end_ascii_offset)
            if line_end_offset < 0:
                raise ValueError("'endASCII:' 라인의 종료(개행)를 찾을 수 없습니다.")

            data_offset = line_end_offset + 1
            ascii_metadata_bytes = mm[begin_header_offset:data_offset]
            ascii_metadata_text, detected_encoding = _decode_metadata_text(
                ascii_metadata_bytes, metadata_encoding
            )

            global_meta: dict[str, Any] = {}
            modules: list[dict[str, str]] = []
            channels: list[dict[str, str]] = []

            current_module: dict[str, str] | None = None
            current_channel: dict[str, str] | None = None

            for raw_line in ascii_metadata_text.replace("\r\n", "\n").split("\n"):
                line = raw_line.strip()
                if not line:
                    continue

                if line.startswith("beginmodule:"):
                    current_module = {"module_index": line.split(":", 1)[1].strip()}
                    continue
                if line == "endmodule:":
                    if current_module is not None:
                        modules.append(current_module)
                    current_module = None
                    continue

                if line.startswith("beginchannel:"):
                    current_channel = {"channel_index": line.split(":", 1)[1].strip()}
                    continue
                if line == "endchannel:":
                    if current_channel is not None:
                        channels.append(current_channel)
                    current_channel = None
                    continue

                key_value = _split_key_value(line)
                if key_value is None:
                    continue
                key, value = key_value

                if current_channel is not None:
                    current_channel[key] = value
                elif current_module is not None:
                    current_module[key] = value
                else:
                    _append_value(global_meta, key, value)

            frames = global_meta.get("frames")
            clk = global_meta.get("clk")
            typ = global_meta.get("typ")

            return {
                "file": str(dat_path),
                "file_size": file_size,
                "magic": magic,
                "metadata_encoding": detected_encoding,
                "begin_header_offset": begin_header_offset,
                "data_offset": data_offset,
                "binary_data_size": file_size - data_offset,
                "summary": {
                    "clk": clk,
                    "typ": typ,
                    "frames": frames,
                    "module_count": len(modules),
                    "channel_count": len(channels),
                },
                "global_meta": global_meta,
                "modules": modules,
                "channels": channels,
                "ascii_metadata_text": ascii_metadata_text,
            }
        finally:
            mm.close()


def write_channels_csv(channels: list[dict[str, str]], out_path: Path) -> None:
    key_order = [
        "channel_index",
        "name",
        "type",
        "unit",
        "module",
        "moduleName",
    ]
    all_keys: list[str] = []
    seen: set[str] = set()
    for key in key_order:
        if any(key in channel for channel in channels):
            all_keys.append(key)
            seen.add(key)
    for channel in channels:
        for key in channel:
            if key not in seen:
                seen.add(key)
                all_keys.append(key)

    with out_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(channels)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PDA(.dat) 파일의 ASCII 메타데이터를 파싱해 요약/JSON/CSV로 내보냅니다."
    )
    parser.add_argument("dat_file", type=Path, help="입력 DAT 파일 경로")
    parser.add_argument(
        "--metadata-encoding",
        choices=["auto", "utf-8", "euc-kr", "cp949", "latin-1"],
        default="auto",
        help="메타데이터 해석 인코딩 (기본: auto)",
    )
    parser.add_argument(
        "--header-out",
        type=Path,
        help="ASCII 메타데이터 원문을 저장할 텍스트 파일 경로",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="파싱 결과 JSON 저장 경로",
    )
    parser.add_argument(
        "--channels-csv-out",
        type=Path,
        help="채널 목록 CSV 저장 경로",
    )
    parser.add_argument(
        "--show-channels",
        type=int,
        default=10,
        help="터미널에 미리보기로 출력할 채널 개수 (기본: 10)",
    )
    args = parser.parse_args()

    result = parse_pda_dat(args.dat_file, metadata_encoding=args.metadata_encoding)

    summary = result["summary"]
    print(f"[파일] {result['file']}")
    print(f"[크기] {result['file_size']:,} bytes")
    print(f"[시그니처] {result['magic']}")
    print(f"[메타데이터 인코딩] {result['metadata_encoding']}")
    print(f"[헤더 시작 오프셋] {result['begin_header_offset']:,}")
    print(f"[데이터 시작 오프셋] {result['data_offset']:,}")
    print(f"[바이너리 데이터 크기] {result['binary_data_size']:,} bytes")
    print(
        "[요약] "
        f"clk={summary['clk']}, typ={summary['typ']}, frames={summary['frames']}, "
        f"modules={summary['module_count']}, channels={summary['channel_count']}"
    )

    channels: list[dict[str, str]] = result["channels"]
    preview_count = max(0, min(args.show_channels, len(channels)))
    if preview_count:
        print(f"\n[채널 미리보기: {preview_count}개]")
        for channel in channels[:preview_count]:
            channel_index = channel.get("channel_index", "")
            name = channel.get("name", "")
            channel_type = channel.get("type", "")
            unit = channel.get("unit", "")
            print(
                f"- idx={channel_index:>5} | name={name} | type={channel_type} | unit={unit}"
            )

    if args.header_out:
        args.header_out.write_text(result["ascii_metadata_text"], encoding="utf-8")
        print(f"\n[완료] 헤더 원문 저장: {args.header_out}")

    if args.json_out:
        json_ready = dict(result)
        json_ready.pop("ascii_metadata_text", None)
        args.json_out.write_text(
            json.dumps(json_ready, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[완료] JSON 저장: {args.json_out}")

    if args.channels_csv_out:
        write_channels_csv(channels, args.channels_csv_out)
        print(f"[완료] 채널 CSV 저장: {args.channels_csv_out}")


if __name__ == "__main__":
    main()
