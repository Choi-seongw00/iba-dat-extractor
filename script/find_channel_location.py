#!/usr/bin/env python3
"""
dat 파일에서 채널명으로 Group / Module / 태그 참조번호를 찾는 스크립트.

태그 참조 규칙 (ibaPDA):
  - 아날로그 채널: [모듈번호:N]  (콜론)  N = 모듈 내 아날로그 순번 (0-based)
  - 디지털 채널:  [모듈번호.N]  (점)    N = 모듈 내 디지털 순번 (0-based)
  - 순번은 채널 idx 오름차순 기준
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from parse_pda_dat import parse_pda_dat


def _assign_tag_refs(channels_in_module: list[dict]) -> dict[int, str]:
    """
    모듈에 속한 채널 목록(idx 오름차순)을 받아
    idx → tag_ref 매핑을 반환한다.
    """
    ana_idx = dig_idx = 0
    result = {}
    for ch in sorted(channels_in_module, key=lambda c: int(c["channel_index"])):
        idx = int(ch["channel_index"])
        mod_num = ch["_module_num"]
        if "digchannel" in ch:
            result[idx] = f"[{mod_num}.{dig_idx}]"
            dig_idx += 1
        else:
            result[idx] = f"[{mod_num}:{ana_idx}]"
            ana_idx += 1
    return result


def build_location_index(
    global_meta: dict,
    modules: list[dict],
    channels: list[dict],
) -> dict:
    """
    채널 idx → location dict 매핑 구성.

    모듈 결정 방식:
      1. small-idx 채널: idx // 64 가 알려진 모듈 번호 → 직접 사용
      2. large-idx 채널: 같은 그룹의 small-idx 앵커 모듈들의
         subindex 범위(signalCount 기반)로 추정
    """
    # group 번호 → 이름
    group_names: dict[int, str] = {}
    for key, val in global_meta.items():
        if key.startswith("Group_name_"):
            g = int(key.split("_")[-1])
            group_names[g] = (val if isinstance(val, str) else val[0]).strip()

    # module 번호 → (이름, signalCount)
    module_info: dict[int, tuple[str, int]] = {}
    for key, val in global_meta.items():
        if key.startswith("Module_name_"):
            m = int(key.split("_")[-1])
            module_info[m] = ((val if isinstance(val, str) else val[0]).strip(), 0)
    for mod in modules:
        m = int(mod.get("module_index", -1))
        name = mod.get("name", "")
        count = int(mod.get("signalCount", 0))
        module_info[m] = (name, count)

    known_modules = set(module_info.keys())

    # 채널별 group_num, subindex 파싱
    ch_group: dict[int, tuple[int, int]] = {}  # idx → (group_num, subindex)
    for ch in channels:
        idx = int(ch.get("channel_index", -1))
        grp_str = ch.get("group", "")
        if "." in grp_str:
            parts = grp_str.split(".", 1)
            try:
                ch_group[idx] = (int(parts[0]), int(parts[1]))
            except ValueError:
                pass

    # 앵커: 그룹별 { module_num: min_subindex }
    group_mod_anchor: dict[int, dict[int, int]] = {}
    for idx, (gnum, sub) in ch_group.items():
        mod_candidate = idx // 64
        if mod_candidate in known_modules:
            anchors = group_mod_anchor.setdefault(gnum, {})
            if mod_candidate not in anchors or sub < anchors[mod_candidate]:
                anchors[mod_candidate] = sub

    # 그룹별 (start_subindex, module_num) 오름차순
    group_mod_sorted: dict[int, list[tuple[int, int]]] = {
        gnum: sorted((sub, mnum) for mnum, sub in anchors.items())
        for gnum, anchors in group_mod_anchor.items()
    }

    # ── 각 채널에 모듈 번호 할당 ──────────────────────────────────────────────
    # module_num → [채널 dict 목록]
    module_channels: dict[int, list[dict]] = {}

    def _assign(ch: dict, mod_num: int) -> None:
        ch = dict(ch)
        ch["_module_num"] = mod_num
        module_channels.setdefault(mod_num, []).append(ch)

    for ch in channels:
        idx = int(ch.get("channel_index", -1))
        mod_candidate = idx // 64

        if ch.get("group", "") == "":
            # group 필드 없음
            if mod_candidate in known_modules:
                _assign(ch, mod_candidate)
            else:
                _assign(ch, -1)  # 완전 미확인
        elif idx in ch_group:
            gnum, sub = ch_group[idx]
            if mod_candidate in known_modules:
                _assign(ch, mod_candidate)
            else:
                # subindex 기반 추정
                mod_sorted = group_mod_sorted.get(gnum, [])
                mod_num = None
                for start, mnum in mod_sorted:
                    if start <= sub:
                        mod_num = mnum
                    else:
                        break
                _assign(ch, mod_num if mod_num is not None else -1)

    # ── 모듈별로 아날로그/디지털 순번 부여 ───────────────────────────────────
    tag_refs: dict[int, str] = {}
    for mod_num, ch_list in module_channels.items():
        if mod_num == -1:
            for ch in ch_list:
                tag_refs[int(ch["channel_index"])] = "N/A"
        else:
            tag_refs.update(_assign_tag_refs(ch_list))

    # ── 최종 location 구성 ────────────────────────────────────────────────────
    location: dict[int, dict] = {}

    for ch in channels:
        idx = int(ch.get("channel_index", -1))
        tag_ref = tag_refs.get(idx, "N/A")

        # 모듈 번호 역추적
        mod_num = None
        for mnum, ch_list in module_channels.items():
            if any(int(c["channel_index"]) == idx for c in ch_list):
                mod_num = mnum
                break

        if mod_num is None or mod_num == -1:
            location[idx] = {
                "group_num": None, "group_name": "N/A",
                "module_num": None, "module_name": "N/A",
                "tag_ref": "N/A", "name": ch.get("name", ""),
            }
            continue

        mod_name, _ = module_info.get(mod_num, ("???", 0))

        gnum = ch_group.get(idx, (None, None))[0]
        if gnum is None:
            gnum = next(
                (g for g, anchors in group_mod_anchor.items() if mod_num in anchors),
                None,
            )
        group_name = group_names.get(gnum, "N/A") if gnum is not None else "N/A"

        location[idx] = {
            "group_num": gnum,
            "group_name": group_name,
            "module_num": mod_num,
            "module_name": mod_name,
            "tag_ref": tag_ref,
            "name": ch.get("name", ""),
        }

    return location


def find_channel(dat_path: Path, search: str) -> None:
    result = parse_pda_dat(dat_path)
    location = build_location_index(
        result["global_meta"], result["modules"], result["channels"]
    )

    search_lower = search.lower()
    matches = [
        loc for loc in location.values()
        if search_lower in loc["name"].lower()
    ]

    if not matches:
        print(f"'{search}' 에 해당하는 채널을 찾을 수 없습니다.")
        return

    for loc in matches:
        print(
            f"Group   : {loc['group_num']}. {loc['group_name']}\n"
            f"Module  : {loc['module_num']}. {loc['module_name']}\n"
            f"태그참조 : {loc['tag_ref']}\n"
            f"채널명  : {loc['name']}\n"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="dat 파일에서 채널의 Group/Module/태그참조번호를 찾습니다."
    )
    parser.add_argument("dat_file", type=Path, help="입력 DAT 파일 경로")
    parser.add_argument("search", help="검색할 채널명 (부분 일치)")
    args = parser.parse_args()

    find_channel(args.dat_file, args.search)


if __name__ == "__main__":
    main()
