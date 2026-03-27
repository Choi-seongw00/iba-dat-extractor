#!/usr/bin/env python3
"""
tags_matched.md 의 태그 목록을 dat 파일에서 찾아 태그참조([mod:ch])를 출력한다.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from parse_pda_dat import parse_pda_dat
from find_channel_location import build_location_index


def load_tag_names(tags_md_path: Path) -> list[str]:
    text = tags_md_path.read_text(encoding="utf-8")
    ns: dict = {}
    exec(text, ns)  # noqa: S102
    if "iba_tag_names" not in ns:
        raise ValueError(f"iba_tag_names 변수를 찾을 수 없습니다: {tags_md_path}")
    return ns["iba_tag_names"]


def resolve_tags(dat_path: Path, tags_md_path: Path) -> list[dict]:
    tag_names = load_tag_names(tags_md_path)

    result = parse_pda_dat(dat_path)
    location = build_location_index(
        result["global_meta"], result["modules"], result["channels"]
    )

    name_to_loc: dict[str, dict] = {
        loc["name"].lower(): loc for loc in location.values()
    }

    rows = []
    for tag in tag_names:
        loc = name_to_loc.get(tag.lower())
        if loc:
            tag_ref = loc["tag_ref"]
            module = f"{loc['module_num']}. {loc['module_name']}" if loc["module_num"] is not None else "N/A"
            group = f"{loc['group_num']}. {loc['group_name']}" if loc["group_num"] is not None else "N/A"
            rows.append({"tag": tag, "tag_ref": tag_ref, "module": module, "group": group, "found": True})
        else:
            rows.append({"tag": tag, "tag_ref": "NOT FOUND", "module": "", "group": "", "found": False})
    return rows


def print_rows(rows: list[dict]) -> None:
    print(f"{'태그명':<60} {'태그참조':<12} {'모듈'}")
    print("-" * 100)
    for r in rows:
        extra = f"  ({r['group']})" if r['group'] and r['group'] != "N/A" else ""
        print(f"{r['tag']:<60} {r['tag_ref']:<12} {r['module']}{extra}")


def save_md(rows: list[dict], out_path: Path) -> None:
    lines = [
        "| 태그명 | 태그참조 | 모듈 | 그룹 |",
        "| --- | --- | --- | --- |",
    ]
    for r in rows:
        lines.append(f"| {r['tag']} | {r['tag_ref']} | {r['module']} | {r['group']} |")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"저장 완료: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="tags_matched.md 태그 목록의 태그참조번호를 dat 파일에서 찾습니다."
    )
    parser.add_argument("dat_file", type=Path, help="입력 DAT 파일 경로")
    parser.add_argument(
        "--tags",
        type=Path,
        default=Path("tags/tags_matched.md"),
        help="태그 목록 파일 (기본: tags/tags_matched.md)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("tags/tags_resolved.md"),
        help="결과 저장 MD 파일 경로 (기본: tags/tags_resolved.md)",
    )
    args = parser.parse_args()

    rows = resolve_tags(args.dat_file, args.tags)
    print_rows(rows)
    save_md(rows, args.out)


if __name__ == "__main__":
    main()
