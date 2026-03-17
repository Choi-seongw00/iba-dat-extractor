#!/usr/bin/env python3
"""
DAT → Parquet 자동화 웹 UI (Streamlit)
실행: uv run streamlit run app.py
"""
from __future__ import annotations

import ast
import datetime as dt
import difflib
import io
import math
import re
import sys
from pathlib import Path

# script/ 폴더를 모듈 검색 경로에 추가
sys.path.insert(0, str(Path(__file__).parent / "script"))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import streamlit as st

from extract_tags_to_parquet import (
    decode_channel_samples,
    parse_metadata,
    parse_start_time,
)

ROOT = Path(__file__).parent
TAGS_DIR = ROOT / "tags"
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
# 헬퍼 함수
# ─────────────────────────────────────────────

CLK = 0.001  # DAT 파일 기본 클럭 (1ms)


def parse_tags_text(text: str) -> list[str]:
    """태그 파일 텍스트(Python list 형식 또는 줄 단위) → 태그 리스트"""
    rhs = text.split("=", 1)[1].strip() if "=" in text else text.strip()
    try:
        parsed = ast.literal_eval(rhs)
        if isinstance(parsed, list):
            return [str(t).strip() for t in parsed if str(t).strip()]
    except Exception:
        pass
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def extract_dat_to_table(
    raw: bytes,
    tag_to_channel: dict[str, str],
    step: int,
) -> tuple[pa.Table, list[str]]:
    """
    단일 DAT 바이트 → (PyArrow Table, 누락 태그 목록)
    tag_to_channel: {원본 태그명: DAT 채널명} 매핑 딕셔너리
    컬럼명은 원본 태그명을 사용.
    """
    global_meta, channels = parse_metadata(raw)
    channel_map = {ch.get("name", ""): ch for ch in channels if ch.get("name")}

    start_time = parse_start_time(global_meta["starttime"])
    clk = float(global_meta.get("clk", CLK))
    frames = int(global_meta["frames"])

    target_frames = list(range(0, frames, step))
    times = [start_time + dt.timedelta(seconds=f * clk) for f in target_frames]

    columns: dict[str, list] = {"time": times}
    missing: list[str] = []

    for orig_tag, ch_name in tag_to_channel.items():
        channel = channel_map.get(ch_name)
        if channel is None:
            missing.append(orig_tag)
            continue
        if str(channel.get("$PDA_Typ", "")).lower() == "text":
            missing.append(orig_tag)
            continue
        columns[orig_tag] = decode_channel_samples(raw, channel, target_frames, master_clk=clk)

    return pa.table(columns), missing


def get_channel_names_from_raw(raw: bytes) -> list[str]:
    """DAT 바이너리에서 채널명 목록만 추출"""
    _, channels = parse_metadata(raw)
    return [ch.get("name", "") for ch in channels if ch.get("name")]


def collect_dat_files(folder_path: str) -> list[Path]:
    return sorted(Path(folder_path).rglob("*.dat"))


def _to_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    converted = float(text)
    if math.isnan(converted):
        return None
    return converted


def parse_correction_rules(rule_rows: object) -> tuple[list[dict[str, float | str]], list[str]]:
    """
    UI 규칙행을 전처리 규칙으로 파싱.
    rule format: {"name": str, "min": float|None, "max": float|None, "replace": float}
    """
    rules: list[dict[str, float | str]] = []
    errors: list[str] = []

    if hasattr(rule_rows, "to_dict"):
        rows = rule_rows.to_dict(orient="records")
    elif isinstance(rule_rows, list):
        rows = rule_rows
    else:
        rows = []

    for idx, row in enumerate(rows, start=1):
        enabled = bool(row.get("enabled", True))
        if not enabled:
            continue

        name = str(row.get("name", f"rule_{idx}")).strip() or f"rule_{idx}"
        min_value = _to_float_or_none(row.get("min"))
        max_value = _to_float_or_none(row.get("max"))

        replace_raw = row.get("replace")
        if replace_raw is None or str(replace_raw).strip() == "":
            errors.append(f"규칙 {idx}({name}): replace 값이 비어 있습니다.")
            continue
        try:
            replace_value = float(replace_raw)
        except ValueError:
            errors.append(f"규칙 {idx}({name}): replace 값이 숫자가 아닙니다.")
            continue

        if min_value is None and max_value is None:
            errors.append(f"규칙 {idx}({name}): min/max 중 하나는 입력해야 합니다.")
            continue

        if min_value is not None and max_value is not None and min_value > max_value:
            errors.append(f"규칙 {idx}({name}): min이 max보다 큽니다.")
            continue

        rules.append({
            "name": name,
            "min": min_value,
            "max": max_value,
            "replace": replace_value,
        })

    return rules, errors


def apply_correction_rules(
    table: pa.Table,
    rules: list[dict[str, float | str]],
    exclude_columns: set[str] | None = None,
) -> tuple[pa.Table, int]:
    """
    전처리 규칙을 테이블 전체에 적용.
    - 규칙은 순서대로 적용
    - 수치 컬럼에만 적용
    - 대용량을 위해 chunk 단위 Arrow compute 사용
    Returns: (보정된 table, 치환된 값 개수)
    """
    if not rules:
        return table, 0

    excluded = exclude_columns or {"time"}
    replaced_total = 0
    new_columns: list[pa.ChunkedArray] = []

    for column_name in table.column_names:
        col = table[column_name]

        if column_name in excluded:
            new_columns.append(col)
            continue

        if not (pa.types.is_floating(col.type) or pa.types.is_integer(col.type)):
            new_columns.append(col)
            continue

        processed_chunks: list[pa.Array] = []
        for chunk in col.iterchunks():
            arr = chunk
            if pa.types.is_integer(arr.type):
                arr = pc.cast(arr, pa.float64())

            for rule in rules:
                mask = None
                min_value = rule["min"]
                max_value = rule["max"]
                replace_value = float(rule["replace"])

                if min_value is not None:
                    cond_min = pc.greater_equal(arr, pa.scalar(float(min_value), type=arr.type))
                    mask = cond_min if mask is None else pc.and_(mask, cond_min)
                if max_value is not None:
                    cond_max = pc.less_equal(arr, pa.scalar(float(max_value), type=arr.type))
                    mask = cond_max if mask is None else pc.and_(mask, cond_max)

                if mask is None:
                    continue

                mask = pc.fill_null(mask, False)
                replaced_count = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
                replaced_total += int(replaced_count)
                arr = pc.if_else(mask, pa.scalar(replace_value, type=arr.type), arr)

            processed_chunks.append(arr)

        new_columns.append(pa.chunked_array(processed_chunks))

    corrected = pa.table(new_columns, names=table.column_names)
    return corrected, replaced_total


# ─────────────────────────────────────────────
# 태그 매칭 (퍼지)
# ─────────────────────────────────────────────

_STRIP_PREFIX = re.compile(r"^.*?\]")
_NON_ALNUM = re.compile(r"[^a-z0-9가-힣]+")
_PREFIX_STRIP_ORDER = (
    ("Groups.",),
    ("S7_EXPLORER.", "S7-EXPLORER.", "S7-EXPLOREP."),
)
_PREFIX_STRIP_PATTERNS = (
    re.compile(r"^\.?M\d{2}\.\d+\s+ACM1\b\s*", re.IGNORECASE),
)


def _strip_known_prefixes(name: str) -> str:
    """알려진 접두사를 지정된 순서대로 제거한다."""
    s = name.strip()
    for prefix_group in _PREFIX_STRIP_ORDER:
        while True:
            for prefix in prefix_group:
                if s.startswith(prefix):
                    s = s[len(prefix):].strip()
                    break
            else:
                break

    for pattern in _PREFIX_STRIP_PATTERNS:
        while True:
            updated = pattern.sub("", s, count=1).strip()
            if updated == s:
                break
            s = updated
    return s


def _normalize(name: str) -> str:
    """채널명/태그명 정규화: 접두사 제거 + 소문자 + 공백/특수문자 통일"""
    s = _STRIP_PREFIX.sub("", name).strip()  # [...] 접두사 제거
    s = _strip_known_prefixes(s)
    s = s.replace("_", " ")
    s = _NON_ALNUM.sub(" ", s.lower()).strip()
    return s


def _score(a: str, b: str) -> tuple[float, float, float]:
    """정규화된 두 문자열의 유사도 점수 → (문자유사도, 토큰겹침, 최종점수)"""
    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if tokens_a and tokens_b:
        overlap = len(tokens_a & tokens_b) / max(len(tokens_a), len(tokens_b))
    else:
        overlap = 0.0
    return ratio, overlap, 0.6 * ratio + 0.4 * overlap


def match_tags_to_channels(
    tags: list[str],
    channel_names: list[str],
    threshold: float = 0.95,
) -> tuple[dict[str, str], list[tuple[str, str, float]], dict[str, tuple[float, float, float]]]:
    """
    태그 목록을 채널명 목록에 퍼지 매칭.
    Returns:
        matched: {original_tag: matched_channel_name}
        low_conf: [(original_tag, best_match, final_score)]  score < threshold인 것
        score_details: {original_tag: (문자유사도, 토큰겹침, 최종점수)}
    """
    norm_channels = [(ch, _normalize(ch)) for ch in channel_names]
    matched: dict[str, str] = {}
    low_conf: list[tuple[str, str, float]] = []
    score_details: dict[str, tuple[float, float, float]] = {}

    for tag in tags:
        # 1) 정확 일치
        if tag in channel_names:
            matched[tag] = tag
            score_details[tag] = (1.0, 1.0, 1.0)
            continue

        # 2) 퍼지 매칭
        norm_tag = _normalize(tag)
        best_ch, best_final = "", 0.0
        best_ratio, best_overlap = 0.0, 0.0
        for ch, norm_ch in norm_channels:
            ratio, overlap, final = _score(norm_tag, norm_ch)
            if final > best_final:
                best_final, best_ch = final, ch
                best_ratio, best_overlap = ratio, overlap

        if best_ch:
            matched[tag] = best_ch
            score_details[tag] = (best_ratio, best_overlap, best_final)
            if best_final < threshold:
                low_conf.append((tag, best_ch, best_final))

    return matched, low_conf, score_details


def _parse_csv_keywords(text: str) -> list[str]:
    return [token.strip() for token in text.split(",") if token.strip()]


def _compile_like_pattern(keyword: str) -> re.Pattern[str]:
    escaped = re.escape(keyword)
    body = escaped.replace(r"\%", ".*").replace(r"\_", ".")
    if "%" not in keyword and "_" not in keyword:
        body = f".*{body}.*"
    return re.compile(body, re.IGNORECASE)


def search_channels_like(
    channel_names: list[str],
    keywords_csv: str,
    match_mode: str = "OR",
    per_keyword_limit: int = 200,
) -> tuple[list[dict[str, str]], list[str], int]:
    keywords = _parse_csv_keywords(keywords_csv)
    if not keywords:
        return [], [], 0

    patterns = [(keyword, _compile_like_pattern(keyword)) for keyword in keywords]

    if match_mode.upper() == "AND":
        matched_channels = [
            channel
            for channel in channel_names
            if all(pattern.search(channel) for _, pattern in patterns)
        ]
        no_match_keywords = [
            keyword
            for keyword, pattern in patterns
            if not any(pattern.search(channel) for channel in channel_names)
        ]

        rows = [
            {"검색어": " AND ".join(keywords), "채널명": channel}
            for channel in matched_channels[:per_keyword_limit]
        ]
        overflow_count = len(matched_channels) - per_keyword_limit
        if overflow_count > 0:
            rows.append({"검색어": " AND ".join(keywords), "채널명": f"... {overflow_count}건 더 있음"})
        return rows, no_match_keywords, len(matched_channels)

    rows: list[dict[str, str]] = []
    no_match_keywords: list[str] = []
    total_hits = 0

    for keyword, pattern in patterns:
        matched = [channel for channel in channel_names if pattern.search(channel)]
        total_hits += len(matched)

        if not matched:
            no_match_keywords.append(keyword)
            continue

        for channel in matched[:per_keyword_limit]:
            rows.append({"검색어": keyword, "채널명": channel})

        overflow_count = len(matched) - per_keyword_limit
        if overflow_count > 0:
            rows.append({"검색어": keyword, "채널명": f"... {overflow_count}건 더 있음"})

    return rows, no_match_keywords, total_hits


# ─────────────────────────────────────────────
# 페이지 레이아웃
# ─────────────────────────────────────────────

st.set_page_config(page_title="DAT → Parquet 추출기", page_icon="📦", layout="wide")
st.title("📦 DAT → Parquet 추출기")
st.caption("PDA2 형식 `.dat` 파일에서 시계열 태그 데이터를 추출합니다.")

with st.expander("📋 태그 파일 작성 방법", expanded=False):
    st.markdown("""
### 태그 파일 형식 안내

DAT 파일 안의 채널명과 태그 파일의 이름이 달라도 **자동 매칭**을 수행합니다.  
가능하면 DAT 채널명과 유사한 이름을 사용할수록 매칭 정확도가 높아집니다.

---

#### 형식 1 — 줄 단위 텍스트 (`.txt`, 권장)
한 줄에 태그 하나씩 작성합니다. `#`으로 시작하는 줄은 주석으로 무시됩니다.
```
# 펌프 관련 태그
Groups.S7_EXPLORER.Pump1 Speed
Groups.S7_EXPLORER.Valve2 Open
Groups.S7_EXPLORER.Motor Current
```

#### 형식 2 — Python 리스트 (`.md`, `.txt`)
`변수명 = [...]` 형태로 작성합니다.
```python
iba_tag_names = [
    "Groups.S7_EXPLORER.Pump1 Speed",
    "Groups.S7_EXPLORER.Valve2 Open",
    "Groups.S7_EXPLORER.Motor Current",
]
```

---

#### 태그명 작성 팁
- DAT 파일의 실제 채널명을 모를 경우 **유사한 키워드**만 써도 자동 매칭이 시도됩니다.
- 자동 매칭 후 매핑 결과를 확인하고, 저신뢰 항목은 직접 수정할 수 있습니다.
- 정확한 채널명을 알고 있다면 그대로 작성하면 됩니다 (정확 일치 우선).
""")

# ─────────────────────────────────────────────
# Section 1: DAT 파일 선택
# ─────────────────────────────────────────────
st.header("1. DAT 파일 선택")

dat_input_mode = st.radio(
    "입력 방식",
    ["📁 로컬 폴더 경로", "⬆️ 파일 직접 업로드"],
    horizontal=True,
    help="로컬 폴더: 지정 경로의 .dat를 모두 처리합니다. 파일 업로드: 선택한 파일만 처리합니다.",
)

dat_files_raw: list[tuple[str, bytes]] = []  # (파일명, 바이트)
dat_input_valid = False

if dat_input_mode == "📁 로컬 폴더 경로":
    folder_input = st.text_input(
        "폴더 경로",
        value=str(Path.cwd() / "data"),
        placeholder="/path/to/data",
    )
    if folder_input:
        folder_path = Path(folder_input)
        if folder_path.is_dir():
            found = collect_dat_files(folder_input)
            if found:
                st.success(f"✅ {len(found)}개 DAT 파일 발견")
                with st.expander("파일 목록 보기"):
                    for f in found:
                        st.text(str(f))
                dat_files_raw = [(str(f), None) for f in found]  # lazy read
                dat_input_valid = True
            else:
                st.warning("⚠️ 해당 폴더에 .dat 파일이 없습니다.")
        elif folder_path.is_file() and folder_path.suffix.lower() == ".dat":
            st.success("✅ DAT 파일 1개 선택됨")
            dat_files_raw = [(str(folder_path), None)]
            dat_input_valid = True
        else:
            st.error("❌ 유효한 폴더 또는 .dat 파일 경로가 아닙니다.")

else:  # 파일 업로드
    uploaded_dats = st.file_uploader(
        "DAT 파일 업로드 (복수 선택 가능)",
        type=["dat"],
        accept_multiple_files=True,
    )
    if uploaded_dats:
        st.success(f"✅ {len(uploaded_dats)}개 DAT 파일 업로드됨")
        dat_files_raw = [(f.name, f.read()) for f in uploaded_dats]
        dat_input_valid = True

# ─────────────────────────────────────────────
# Section 2: 태그 설정
# ─────────────────────────────────────────────
st.header("2. 태그 설정")

tag_input_mode = st.radio(
    "태그 소스",
    ["📄 파일 경로 입력", "⬆️ 파일 업로드", "✏️ 직접 입력"],
    horizontal=True,
)

tags: list[str] = []
tag_input_valid = False

if tag_input_mode == "📄 파일 경로 입력":
    default_tag_file = str(TAGS_DIR / "tags.md")
    tag_file_input = st.text_input(
        "태그 파일 경로",
        value=default_tag_file,
        placeholder="tags/tags_matched.txt 또는 tags/tags.md 경로",
    )
    if tag_file_input:
        tag_path = Path(tag_file_input)
        if tag_path.is_file():
            text = tag_path.read_text(encoding="utf-8", errors="replace")
            tags = parse_tags_text(text)
            if tags:
                st.success(f"✅ {len(tags)}개 태그 로드됨")
                tag_input_valid = True
            else:
                st.error("❌ 태그를 파싱할 수 없습니다.")
        else:
            st.error("❌ 파일을 찾을 수 없습니다.")

elif tag_input_mode == "⬆️ 파일 업로드":
    uploaded_tag = st.file_uploader("태그 파일 업로드 (.txt 또는 .md)", type=["txt", "md"])
    if uploaded_tag:
        text = uploaded_tag.read().decode("utf-8", errors="replace")
        tags = parse_tags_text(text)
        if tags:
            st.success(f"✅ {len(tags)}개 태그 로드됨")
            tag_input_valid = True
        else:
            st.error("❌ 태그를 파싱할 수 없습니다.")

else:  # 직접 입력
    tag_text = st.text_area(
        "태그명 입력 (한 줄에 하나씩)",
        height=200,
        placeholder="Groups.S7_EXPLORER.Pump1\nGroups.S7_EXPLORER.Valve2\n...",
    )
    if tag_text.strip():
        tags = [line.strip() for line in tag_text.splitlines() if line.strip()]
        if tags:
            st.success(f"✅ {len(tags)}개 태그 입력됨")
            tag_input_valid = True

if tags:
    with st.expander("태그 목록 미리보기"):
        for i, t in enumerate(tags, 1):
            st.text(f"{i:>3}. {t}")

# ─────────────────────────────────────────────
# Section 2-B: 태그 매칭 (DAT 샘플로 미리보기)
# ─────────────────────────────────────────────
tag_to_channel: dict[str, str] = {t: t for t in tags}  # 기본: 동일명 사용
matching_done = False

if tags and dat_input_valid:
    st.subheader("태그 매칭 확인")
    st.caption("DAT 파일의 실제 채널명과 입력 태그를 매칭합니다. 정확 일치하지 않으면 퍼지 매칭을 수행합니다.")

    # 첫 번째 DAT에서 채널명 샘플링
    first_fname, first_raw = dat_files_raw[0]
    with st.spinner("채널 목록 로딩 중..."):
        try:
            if first_raw is None:
                first_raw = Path(first_fname).read_bytes()
            channel_names = get_channel_names_from_raw(first_raw)
        except Exception as e:
            channel_names = []
            st.error(f"채널 목록 로딩 실패: {e}")

    if channel_names:
        st.markdown("##### 채널명 LIKE 검색")
        st.caption("`,`로 여러 검색어를 입력하면 AND 조건으로 검색합니다. `%`, `_` 와일드카드를 지원합니다.")

        keyword_query = st.text_input(
            "검색어",
            value="",
            placeholder="FRONT CLAMP LEFT, ROCKER ARM, %FEEDING%",
            key="channel_like_search",
        )
        if keyword_query.strip():
            rows, no_match_keywords, total_hits = search_channels_like(
                channel_names,
                keyword_query,
                match_mode="AND",
            )
            if rows:
                import pandas as pd

                st.dataframe(pd.DataFrame(rows), use_container_width=True, height=260)
                st.info(f"LIKE AND 검색 결과: {total_hits}건")
            else:
                st.warning("검색어에 해당하는 채널명이 없습니다.")

            if no_match_keywords:
                st.caption(f"미일치 검색어: {', '.join(no_match_keywords)}")

        exact_match_count = sum(1 for t in tags if t in channel_names)
        fuzzy_needed = len(tags) - exact_match_count

        if fuzzy_needed == 0:
            st.success(f"✅ 모든 태그({len(tags)}개)가 정확 일치합니다.")
            tag_to_channel = {t: t for t in tags}
            matching_done = True
        else:
            if exact_match_count > 0:
                st.info(f"정확 일치: {exact_match_count}개 / 퍼지 매칭 필요: {fuzzy_needed}개")
            else:
                st.warning(f"⚠️ 정확 일치 태그가 없습니다. 퍼지 매칭을 수행합니다. ({len(tags)}개 태그)")

            run_match_btn = st.button("🔍 태그 자동 매칭 실행", key="match_btn")
            if run_match_btn or st.session_state.get("match_result"):
                if run_match_btn:
                    with st.spinner("매칭 중..."):
                        result, low_conf, score_details = match_tags_to_channels(tags, channel_names)
                    st.session_state["match_result"] = result
                    st.session_state["match_low_conf"] = low_conf
                    st.session_state["match_score_details"] = score_details
                else:
                    result = st.session_state["match_result"]
                    low_conf = st.session_state["match_low_conf"]
                    score_details = st.session_state["match_score_details"]

                tag_to_channel = result
                matching_done = True

                # 매칭 결과 테이블
                import pandas as pd
                low_conf_tags = {o for o, _, _ in low_conf}
                rows = []
                for orig, ch in result.items():
                    r, ov, final = score_details.get(orig, (1.0, 1.0, 1.0))
                    rows.append({
                        "입력 태그": orig,
                        "매칭된 채널명": ch,
                        "문자유사도": round(r, 3),
                        "토큰겹침": round(ov, 3),
                        "최종점수": round(final, 3),
                        "상태": "⚠️ 저신뢰" if orig in low_conf_tags else "✅ 정상",
                    })
                df_match = pd.DataFrame(rows)
                st.dataframe(df_match, use_container_width=True)

                if low_conf:
                    st.warning(f"⚠️ 저신뢰 매칭 {len(low_conf)}개 — 결과를 확인하고 필요 시 태그명을 수정하세요.")
                else:
                    st.success("✅ 모든 태그 매칭 완료")
    else:
        st.warning("채널 목록을 읽을 수 없어 매칭을 건너뜁니다.")
        matching_done = True
elif tags:
    matching_done = True  # DAT 없이 태그만 입력된 경우 매칭 스킵

# ─────────────────────────────────────────────
# Section 3: 추출 간격
# ─────────────────────────────────────────────
st.header("3. 추출 간격")

col1, col2 = st.columns([2, 1])
with col1:
    interval_preset = st.radio(
        "시간 간격",
        ["0.1초", "0.5초", "1초", "5초", "10초", "직접 입력"],
        horizontal=True,
        index=2,
    )

interval_sec: float
if interval_preset == "직접 입력":
    with col2:
        interval_sec = st.number_input(
            "간격(초)",
            min_value=0.001,
            max_value=3600.0,
            value=1.0,
            step=0.1,
            format="%.3f",
        )
else:
    interval_sec = float(interval_preset.replace("초", ""))

step = max(1, round(interval_sec / CLK))
st.caption(f"step = {step} frames (간격 = {interval_sec}초, clk = {CLK}s)")

# ─────────────────────────────────────────────
# Section 4: 값 보정(전처리)
# ─────────────────────────────────────────────
st.header("4. 값 보정(전처리)")

st.caption("여러 조건의 값 보정 규칙을 순서대로 적용합니다. (대용량 데이터에 맞게 Arrow 벡터 연산으로 처리)")

if "correction_rule_rows" not in st.session_state:
    st.session_state["correction_rule_rows"] = [
        {
            "enabled": True,
            "name": "deadband_zero",
            "min": -0.00001,
            "max": 0.00001,
            "replace": 0.0,
        }
    ]

rule_rows = st.data_editor(
    st.session_state["correction_rule_rows"],
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "enabled": st.column_config.CheckboxColumn("사용", default=True),
        "name": st.column_config.TextColumn("규칙명"),
        "min": st.column_config.NumberColumn("최소값(이상)", format="%.10f"),
        "max": st.column_config.NumberColumn("최대값(이하)", format="%.10f"),
        "replace": st.column_config.NumberColumn("치환값", format="%.10f"),
    },
)
st.session_state["correction_rule_rows"] = rule_rows

correction_rules, correction_rule_errors = parse_correction_rules(rule_rows)
if correction_rule_errors:
    for err in correction_rule_errors:
        st.error(err)
else:
    st.success(f"✅ 활성 전처리 규칙 {len(correction_rules)}개")

# ─────────────────────────────────────────────
# Section 5: 출력 설정
# ─────────────────────────────────────────────
st.header("5. 출력 설정")

st.caption(
    f"출력 위치: `{OUTPUT_DIR}/`  "
    "파일명은 추출 완료 후 데이터 시간 범위 기준으로 자동 생성됩니다.  "
    "형식: `output_{{생성일자}}_{{데이터시작}}_{{데이터종료}}.parquet`"
)

# ─────────────────────────────────────────────
# 미리보기
# ─────────────────────────────────────────────
st.divider()
st.subheader("📊 데이터 미리보기")
st.caption("첫 번째 파일의 샘플 데이터를 미리 확인할 수 있습니다.")

col_preview_btn, col_run_btn = st.columns(2)

preview_btn = col_preview_btn.button(
    "📊 미리보기",
    disabled=not (dat_input_valid and tag_input_valid and matching_done and not correction_rule_errors),
    use_container_width=True,
)

run_btn = col_run_btn.button(
    "🚀 추출 실행",
    type="primary",
    disabled=not (dat_input_valid and tag_input_valid and matching_done and not correction_rule_errors),
    use_container_width=True,
)

# ─────────────────────────────────────────────
# 미리보기 로직
# ─────────────────────────────────────────────
if preview_btn:
    st.info("첫 번째 파일의 샘플 데이터를 처리 중입니다...")
    try:
        first_fname, first_raw_bytes = dat_files_raw[0]
        if first_raw_bytes is None:
            first_raw_bytes = Path(first_fname).read_bytes()
        
        # 첫 파일만 추출
        table, missing = extract_dat_to_table(first_raw_bytes, tag_to_channel, step)
        
        # 전처리 규칙 적용
        table, replaced_count = apply_correction_rules(table, correction_rules, exclude_columns={"time"})
        
        st.success(f"✅ 미리보기 완료 ({first_fname})")
        
        # 통계
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        col_m1.metric("파일명", first_fname.split('/')[-1])
        col_m2.metric("행 수", f"{table.num_rows:,}")
        col_m3.metric("컬럼 수", table.num_columns)
        col_m4.metric("치환된 값", f"{replaced_count:,}")
        
        if missing:
            st.warning(f"⚠️ 누락된 태그 {len(missing)}개: {', '.join(missing[:5])}")
        
        # 데이터 미리보기 (상위 100행)
        st.subheader("데이터 샘플 (상위 100행)")
        df_preview = table.slice(0, min(100, table.num_rows)).to_pandas()
        st.dataframe(df_preview, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ 미리보기 실패: {e}")

if run_btn:
    result_buf = io.BytesIO()
    all_missing: set[str] = set()
    errors: list[str] = []
    replaced_values_total = 0
    processed_files = 0
    total_rows = 0
    preview_tables: list[pa.Table] = []
    preview_rows_target = 100
    current_schema: pa.Schema | None = None
    min_time: dt.datetime | None = None
    max_time: dt.datetime | None = None

    tmp_output = OUTPUT_DIR / f"_tmp_{dt.datetime.now().strftime('%Y%m%d%H%M%S%f')}.parquet"
    writer: pq.ParquetWriter | None = None

    total = len(dat_files_raw)
    progress_bar = st.progress(0, text="처리 중...")
    log_box = st.empty()

    for idx, (fname, raw_bytes) in enumerate(dat_files_raw):
        log_box.info(f"처리 중 ({idx + 1}/{total}): {fname}")
        try:
            if raw_bytes is None:
                raw_bytes = Path(fname).read_bytes()

            table, missing = extract_dat_to_table(raw_bytes, tag_to_channel, step)

            # 값 보정 전처리
            table, replaced_count = apply_correction_rules(table, correction_rules, exclude_columns={"time"})
            replaced_values_total += replaced_count

            # 시간 범위 업데이트 (파일명 생성용)
            file_min_time = pc.min(table["time"]).as_py()
            file_max_time = pc.max(table["time"]).as_py()
            if isinstance(file_min_time, dt.datetime):
                min_time = file_min_time if min_time is None else min(min_time, file_min_time)
            if isinstance(file_max_time, dt.datetime):
                max_time = file_max_time if max_time is None else max(max_time, file_max_time)

            # 대용량 대응: 파일별 스트리밍 Parquet 저장
            if writer is None:
                writer = pq.ParquetWriter(tmp_output, table.schema, compression="zstd")
                current_schema = table.schema
            writer.write_table(table)

            processed_files += 1
            total_rows += table.num_rows
            all_missing.update(missing)

            # 미리보기 100행만 유지
            if preview_rows_target > 0 and table.num_rows > 0:
                take_rows = min(preview_rows_target, table.num_rows)
                preview_tables.append(table.slice(0, take_rows))
                preview_rows_target -= take_rows
        except Exception as exc:
            errors.append(f"{fname}: {exc}")
        finally:
            progress_bar.progress((idx + 1) / total, text=f"처리 중 ({idx + 1}/{total})...")

    progress_bar.progress(1.0, text="병합 중...")

    if writer is not None:
        writer.close()

    if processed_files > 0 and min_time and max_time:
        # 출력 파일명 자동 생성: output_{생성일자}_{데이터시작}_{데이터종료}.parquet
        t_start = min_time.strftime("%Y%m%d%H%M%S")
        t_end = max_time.strftime("%Y%m%d%H%M%S")
        today_str = dt.date.today().strftime("%Y%m%d")
        auto_filename = f"output_{today_str}_{t_start}_{t_end}.parquet"
        output_path = OUTPUT_DIR / auto_filename

        if output_path.exists():
            output_path.unlink()
        tmp_output.replace(output_path)

        # 다운로드용: 너무 큰 파일은 메모리 보호를 위해 비활성
        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        download_enabled = output_size_mb <= 200
        if download_enabled:
            result_buf.write(output_path.read_bytes())
            result_buf.seek(0)

        progress_bar.progress(1.0, text="✅ 완료!")
        log_box.empty()

        st.success(f"✅ 추출 완료! {processed_files}개 파일 처리")
        st.info(f"💾 저장 위치: `{output_path}`")

        # 결과 통계
        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("처리 파일", processed_files)
        col_b.metric("전체 행 수", f"{total_rows:,}")
        col_c.metric("컬럼 수", len(current_schema) if current_schema else 0)
        col_d.metric("누락 태그", len(all_missing))
        st.metric("치환된 값 개수", f"{replaced_values_total:,}")

        if all_missing:
            with st.expander(f"⚠️ 누락된 태그 ({len(all_missing)}개)"):
                for t in sorted(all_missing):
                    st.text(t)

        if errors:
            with st.expander(f"❌ 오류 발생 파일 ({len(errors)}개)"):
                for e in errors:
                    st.text(e)

        # 데이터 미리보기
        st.subheader("데이터 미리보기 (상위 100행)")
        if preview_tables:
            df_preview = pa.concat_tables(preview_tables).to_pandas()
            st.dataframe(df_preview, use_container_width=True)
        else:
            st.info("미리보기 데이터가 없습니다.")

        # 다운로드
        if download_enabled:
            st.download_button(
                label=f"⬇️ {auto_filename} 다운로드",
                data=result_buf,
                file_name=auto_filename,
                mime="application/octet-stream",
                type="primary",
                use_container_width=True,
            )
        else:
            st.info(f"파일 크기 {output_size_mb:.1f}MB로 큰 편이라 브라우저 다운로드 버튼은 생략했습니다. output 폴더 파일을 직접 사용하세요.")
    else:
        if writer is not None:
            writer.close()
        if tmp_output.exists():
            tmp_output.unlink(missing_ok=True)
        progress_bar.empty()
        log_box.error("❌ 처리된 파일이 없습니다.")
        if errors:
            for e in errors:
                st.error(e)
