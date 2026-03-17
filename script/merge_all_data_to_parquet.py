#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from extract_tags_to_parquet import (
    decode_channel_samples,
    parse_metadata,
    parse_start_time,
    read_tags,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="data 폴더의 모든 DAT를 하나의 Parquet 파일로 병합합니다."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="DAT 파일 루트 폴더 (기본: data)",
    )
    parser.add_argument(
        "--tag-file",
        type=Path,
        default=Path("tags_matched.txt"),
        help="태그 파일(tags.md 또는 줄단위 txt)",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=1000,
        help="프레임 간격 (기본 1000=1초)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("all_data_tags_1s.parquet"),
        help="출력 Parquet 파일",
    )
    args = parser.parse_args()

    dat_files = sorted(args.data_dir.rglob("*.dat"))
    if not dat_files:
        raise FileNotFoundError(f"DAT 파일을 찾을 수 없습니다: {args.data_dir}")

    tags = read_tags(args.tag_file)
    step = max(1, args.step)

    columns: dict[str, list[float | dt.datetime | None]] = {"time": []}
    for tag in tags:
        columns[tag] = []

    processed = 0
    skipped = 0
    skipped_files: list[str] = []

    for dat_file in dat_files:
        raw = dat_file.read_bytes()
        global_meta, channels = parse_metadata(raw)

        start_time = parse_start_time(global_meta["starttime"])
        clk = float(global_meta["clk"])
        frames = int(global_meta["frames"])
        target_frames = list(range(0, frames, step))
        times = [start_time + dt.timedelta(seconds=frame * clk) for frame in target_frames]

        channel_map = {channel.get("name", ""): channel for channel in channels if channel.get("name")}

        missing_in_this_file = False
        decoded_per_tag: dict[str, list[float | None]] = {}
        for tag in tags:
            channel = channel_map.get(tag)
            if channel is None:
                missing_in_this_file = True
                decoded_per_tag[tag] = [None] * len(target_frames)
                continue
            if str(channel.get("$PDA_Typ", "")).lower() == "text":
                missing_in_this_file = True
                decoded_per_tag[tag] = [None] * len(target_frames)
                continue
            decoded_per_tag[tag] = decode_channel_samples(raw, channel, target_frames, master_clk=clk)

        if missing_in_this_file:
            skipped_files.append(str(dat_file))

        columns["time"].extend(times)
        for tag in tags:
            columns[tag].extend(decoded_per_tag[tag])

        processed += 1

    table = pa.table(columns)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.output, compression="zstd")

    print(f"[저장 완료] {args.output}")
    print(f"[처리 파일 수] {processed}")
    print(f"[행 수] {table.num_rows}")
    print(f"[컬럼 수] {table.num_columns} (time 포함)")
    print(f"[태그 수] {len(tags)}")
    print(f"[태그 누락/텍스트 포함 파일 수] {len(skipped_files)}")
    if skipped_files:
        print("[예시 파일]", skipped_files[:3])


if __name__ == "__main__":
    main()
