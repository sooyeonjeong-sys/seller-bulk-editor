#!/usr/bin/env python3
"""
지재 신상품 적중상품 임계값 실시간 모니터링
실행: streamlit run hit_product_monitor.py
"""

import warnings
warnings.filterwarnings("ignore", message="BigQuery Storage module not found")

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import time

# ── 페이지 설정 ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="적중상품 임계값 모니터링",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
html, body, [class*="css"] { font-size: 13px !important; }
p, label, .stMarkdown p, span { font-size: 13px; }

button[data-testid="baseButton-primary"],
.stButton > button[kind="primary"] {
    background-color: #642FE9 !important;
    border-color: #642FE9 !important;
    color: #fff !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    border-radius: 6px !important;
    padding: 6px 14px !important;
}
button[data-testid="baseButton-primary"]:hover,
.stButton > button[kind="primary"]:hover {
    background-color: #7B47FF !important;
}
.stButton > button[kind="secondary"] {
    font-size: 12px !important;
    border-radius: 6px !important;
    padding: 6px 14px !important;
}

div[data-testid="stMetricValue"] { font-size: 22px !important; font-weight: 700; }
div[data-testid="stMetricLabel"] { font-size: 11px !important; }

.hit-row { background-color: rgba(80, 200, 120, 0.12) !important; }
.near-row { background-color: rgba(255, 180, 0, 0.10) !important; }

.progress-wrap {
    background: #2a2f3e;
    border-radius: 6px;
    height: 8px;
    overflow: hidden;
    margin-top: 2px;
}
.progress-bar-green  { background: #50C878; height: 8px; border-radius: 6px; }
.progress-bar-orange { background: #FFB400; height: 8px; border-radius: 6px; }
.progress-bar-red    { background: #FF5252; height: 8px; border-radius: 6px; }
.progress-bar-gray   { background: #556; height: 8px; border-radius: 6px; }

.section-header {
    font-size: 14px !important;
    font-weight: 700;
    color: #C8D4F0;
    margin: 12px 0 6px 0;
    border-bottom: 1px solid #2a2f3e;
    padding-bottom: 4px;
}
.last-updated {
    font-size: 11px;
    color: #8899bb;
    text-align: right;
    margin-top: -8px;
    margin-bottom: 8px;
}
/* 썸네일이 있는 셀 수직 정렬 */
[data-testid="stMarkdownContainer"] img {
    display: inline-block !important;
}
</style>
""", unsafe_allow_html=True)

# ── BigQuery 클라이언트 ───────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project="damoa-mart", credentials=creds)
    return bigquery.Client(project="damoa-mart")


def run_query(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


# ls_hit_rate season_label 매핑
SEASON_LABEL_MAP: dict[str, str] = {
    "2026 SS": "26SS",
    "2025 FW": "25FW",
    "2025 SS": "25SS",
    "2024 FW": "24FW",
    "2024 SS": "24SS",
}


# ── 데이터 로드 ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)  # 5분 캐시
def load_monitor_data(brand_filter: str, season: str) -> pd.DataFrame:
    """ls_hit_rate 뷰 직접 조회 + pb_new_product_check LEFT JOIN (주차별 GMV)"""
    season_label = SEASON_LABEL_MAP.get(season, "26SS")
    brand_clause = f"AND lr.brand_name = '{brand_filter}'" if brand_filter != "전체 PB" else ""
    sql = f"""
    SELECT
      lr.brand_name,
      lr.mall_product_code,
      lr.product_id                             AS item_id,
      lr.product_name                           AS display_title,
      lr.thumbnail_url,
      lr.product_type                           AS ls_product_type,
      CAST(lr.gmv_season_total AS INT64)        AS gmv_season_total,
      CAST(lr.gmv_threshold    AS INT64)        AS gmv_threshold,
      lr.is_hit                                 AS is_hit_raw,
      CAST(pnpc.gmv_last_7d    AS INT64)        AS gmv_last_7d,
      CAST(pnpc.gmv_last_8_14d AS INT64)        AS gmv_last_8_14d,
      CAST(pnpc.gmv_w1    AS INT64)             AS gmv_w1,
      CAST(pnpc.gmv_w2    AS INT64)             AS gmv_w2,
      CAST(pnpc.gmv_w3    AS INT64)             AS gmv_w3,
      CAST(pnpc.gmv_w4_6  AS INT64)             AS gmv_w4_6,
      CAST(pnpc.gmv_w7_12 AS INT64)             AS gmv_w7_12,
      pnpc.launch_date,
      pnpc.days_since_launch_total,
      COALESCE(
        pnpc.product_detail_link,
        CONCAT('https://web.queenit.kr/product/', lr.product_id)
      )                                         AS product_detail_link
    FROM `damoa-fb351.pb1.ls_hit_rate` lr
    LEFT JOIN `damoa-mart.pb1.pb_new_product_check` pnpc
      ON lr.mall_product_code = pnpc.mall_product_code
      AND pnpc.season_cohort = '{season}'
      AND pnpc.launch_date IS NOT NULL
    WHERE lr.season_label = '{season_label}'
      AND lr.brand_name NOT IN ('아르앙', '희애', '노어', '브에트와')
      {brand_clause}
    ORDER BY lr.gmv_season_total DESC NULLS LAST
    """
    df = run_query(sql)
    return df


def make_summary(df: pd.DataFrame) -> pd.DataFrame:
    """df의 is_hit 컬럼(실제 임계값 기준) 으로 브랜드별 요약 생성"""
    summary = (
        df.groupby("brand_name")
        .agg(
            total_products=("item_id", "nunique"),
            a_player_count=("is_hit", "sum"),
        )
        .reset_index()
    )
    summary["hit_rate_pct"] = (
        summary["a_player_count"] / summary["total_products"] * 100
    ).round(1)
    return summary.sort_values("hit_rate_pct", ascending=False)


# ── 헬퍼 함수 ────────────────────────────────────────────────────────────────
def progress_html(pct: float) -> str:
    w = min(pct, 100)
    if pct >= 100:
        color = "green"
    elif pct >= 70:
        color = "orange"
    elif pct > 0:
        color = "red"
    else:
        color = "gray"
    return (
        f'<div class="progress-wrap">'
        f'<div class="progress-bar-{color}" style="width:{w}%"></div>'
        f'</div>'
    )


def eta_text(remaining: int, gmv_7d) -> str:
    if remaining <= 0:
        return "✅ 달성"
    if gmv_7d is None or pd.isna(gmv_7d) or gmv_7d <= 0:
        return "-"
    daily = gmv_7d / 7
    days = remaining / daily
    if days < 1:
        return "< 1일"
    return f"~{int(days)}일"


def fmt_won(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    return f"₩{int(v):,}"


# ── 사이드바 ─────────────────────────────────────────────────────────────────
PB_BRANDS = ["전체 PB", "지재", "다나앤페타", "마치마라", "베르다", "퀸즈셀렉션"]

with st.sidebar:
    st.markdown("## 🎯 적중상품 모니터")

    selected_brand = st.selectbox("브랜드", PB_BRANDS, index=1)
    selected_season = st.selectbox("시즌", ["2026 SS", "2025 FW", "2025 SS"], index=0)
    st.divider()

    show_all = st.toggle("적중 상품 포함 전체 표시", value=True)
    top_n = st.slider("상위 N개 표시", 10, 200, 50, step=10)

    st.divider()

    auto_refresh = st.toggle("자동 새로고침", value=False)
    refresh_sec = st.selectbox("새로고침 주기", [30, 60, 120, 300],
                               format_func=lambda x: f"{x}초") if auto_refresh else None

    if st.button("🔄 지금 새로고침", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── 메인 ─────────────────────────────────────────────────────────────────────
st.markdown(f"## 🎯 적중상품 임계값 모니터 — {selected_brand} ({selected_season})")

# 데이터 로드
with st.spinner("BigQuery 조회 중..."):
    df = load_monitor_data(selected_brand, selected_season)

if df.empty:
    st.warning("해당 조건의 상품 데이터가 없습니다.")
    st.stop()

# 임계값 결정 (ls_hit_rate에서 직접 가져옴)
if df["gmv_threshold"].notna().any():
    THRESHOLD = int(df["gmv_threshold"].dropna().iloc[0])
    threshold_source = "ls_hit_rate 직접 조회"
else:
    THRESHOLD = 0
    threshold_source = "데이터 없음"

# 파생 컬럼
df["gmv"] = df["gmv_season_total"].fillna(0).astype(int)
# is_hit: ls_hit_rate.is_hit 직접 사용 (1=적중 상품, 0=미적중)
df["is_hit"] = df["is_hit_raw"].fillna(0).astype(bool)
# product_type: ls_hit_rate.product_type 사용 ("재진행"/"신상품" → "재진행"/"신상")
# + display_title [BEST] 보완 체크
df["product_type"] = (
    df["ls_product_type"].eq("재진행") |
    df["display_title"].str.contains(r"\[BEST\]", na=False) |
    df["mall_product_code"].str[7:8].eq("9")
).map({True: "재진행", False: "신상"})
df["progress_pct"] = (df["gmv"] / THRESHOLD * 100).clip(0, 999)
df["gap"] = df["gmv"] - THRESHOLD
df["remaining"] = (-df["gap"]).clip(lower=0)
df["eta"] = df.apply(lambda r: eta_text(r["remaining"], r.get("gmv_last_7d")), axis=1)

def eta_days_numeric(remaining, gmv_7d) -> float:
    """정렬용 숫자 ETA: 달성=0, 데이터없음=9999"""
    if remaining <= 0:
        return 0.0
    if gmv_7d is None or pd.isna(gmv_7d) or gmv_7d <= 0:
        return 9999.0
    return remaining / (gmv_7d / 7)

df["eta_days"] = df.apply(
    lambda r: eta_days_numeric(r["remaining"], r.get("gmv_last_7d")), axis=1
)

# 브랜드별 요약 (임계값 기준 재계산)
df_summary = make_summary(df)

# ── 세션 상태 ────────────────────────────────────────────────────────────────
if "active_card" not in st.session_state:
    st.session_state.active_card = None
if "sort_mode" not in st.session_state:
    st.session_state.sort_mode = "gmv"

def toggle_card(card_id: str):
    st.session_state.active_card = None if st.session_state.active_card == card_id else card_id


# ── 브랜드별 요약 지표 ────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📊 브랜드별 적중률 현황</div>', unsafe_allow_html=True)

brands_to_show = df_summary if selected_brand == "전체 PB" else df_summary[df_summary["brand_name"] == selected_brand]
cols = st.columns(min(len(brands_to_show), 5))
for i, (_, row) in enumerate(brands_to_show.iterrows()):
    with cols[i % 5]:
        st.metric(
            row["brand_name"],
            f"{row['hit_rate_pct']:.1f}%",
            f"적중 상품 {int(row['a_player_count'])}개 / 전체 {int(row['total_products'])}개"
        )

# ── 클릭형 요약 카드 ──────────────────────────────────────────────────────────
st.divider()

total      = len(df)
hit_count  = int(df["is_hit"].sum())
near_count = int(((df["progress_pct"] >= 70) & ~df["is_hit"]).sum())
zero_count = int((df["gmv"] == 0).sum())

new_df  = df[df["product_type"] == "신상"]
re_df   = df[df["product_type"] == "재진행"]
n_total, n_hit = len(new_df), int(new_df["is_hit"].sum())
r_total, r_hit = len(re_df),  int(re_df["is_hit"].sum())

active = st.session_state.active_card

# 카드 CSS (활성/비활성)
st.markdown("""
<style>
.card-active button { border: 2px solid #642FE9 !important; background-color: rgba(100,47,233,0.15) !important; }
</style>""", unsafe_allow_html=True)

def metric_btn(col, card_id, label, value_str, delta_str=None):
    is_active = active == card_id
    prefix = "● " if is_active else ""
    delta_part = f"  ({delta_str})" if delta_str else ""
    btn_label = f"{prefix}{label}\n**{value_str}**{delta_part}"
    with col:
        if st.button(f"{prefix}{label} {value_str}{delta_part}",
                     key=f"card_{card_id}",
                     use_container_width=True,
                     type="primary" if is_active else "secondary"):
            toggle_card(card_id)
            st.rerun()

# 상단 행: 전체 요약
r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns(5)
metric_btn(r1c1, "all",    f"{selected_season} 전체 상품", f"{total}개")
metric_btn(r1c2, "aplayer","🏆 적중 상품",       f"{hit_count}개",  f"{hit_count/total*100:.1f}%")
metric_btn(r1c3, "near",   "⚡ 70%+ 임박",      f"{near_count}개")
metric_btn(r1c4, "zero",   "🚨 미판매 (0원)",   f"{zero_count}개")
r1c5.metric("임계값", fmt_won(THRESHOLD), threshold_source)

# 하단 행: 신상 / 재진행 분류
r2c1, r2c2, r2c3, r2c4 = st.columns(4)
metric_btn(r2c1, "new_all",     "🆕 신상 전체",       f"{n_total}개")
metric_btn(r2c2, "new_aplayer", "🆕 신상 적중 상품",   f"{n_hit}개",
           f"{n_hit/n_total*100:.1f}%" if n_total else "0%")
metric_btn(r2c3, "re_all",      "🔁 재진행 전체",     f"{r_total}개")
metric_btn(r2c4, "re_aplayer",  "🔁 재진행 적중 상품", f"{r_hit}개",
           f"{r_hit/r_total*100:.1f}%" if r_total else "0%")


# ── 필터링 로직 ───────────────────────────────────────────────────────────────
def apply_status_filter(src: pd.DataFrame) -> pd.DataFrame:
    """active 카드에 따른 상태 필터"""
    if active in ("aplayer", "new_aplayer", "re_aplayer"):
        return src[src["is_hit"]]
    elif active == "near":
        return src[(src["progress_pct"] >= 70) & ~src["is_hit"]]
    elif active == "zero":
        return src[src["gmv"] == 0]
    else:
        return src if show_all else src[~src["is_hit"]]

# 어떤 섹션을 보여줄지 결정
show_new = active not in ("re_all", "re_aplayer")
show_re  = active not in ("new_all", "new_aplayer")

filtered_new = apply_status_filter(new_df) if show_new else pd.DataFrame()
filtered_re  = apply_status_filter(re_df)  if show_re  else pd.DataFrame()


# ── 랭킹 테이블 렌더 함수 ────────────────────────────────────────────────────
def render_ranked_table(section_df: pd.DataFrame, title: str):
    if section_df.empty:
        st.info(f"{title}: 해당 조건의 상품이 없습니다.")
        return

    display = section_df.head(top_n).reset_index(drop=True)
    st.markdown(f'<div class="section-header">{title} ({len(section_df)}개)</div>',
                unsafe_allow_html=True)

    hcols = st.columns([0.4, 3.5, 1.2, 1.2, 1.2, 2.5, 1.0, 1.0])
    hcols[0].markdown("**#**")
    hcols[1].markdown("**상품명**")
    hcols[2].markdown("**누적 GMV**")
    hcols[3].markdown("**임계값 대비**")
    hcols[4].markdown("**달성률**")
    hcols[5].markdown("**진행률**")
    hcols[6].markdown("**ETA**")
    hcols[7].markdown("**상태**")
    st.markdown('<hr style="margin:4px 0; border-color:#2a2f3e;">', unsafe_allow_html=True)

    for rank, (_, row) in enumerate(display.iterrows(), 1):
        pct     = row["progress_pct"]
        gap     = row["gap"]
        gap_str = f"+{gap:,}" if gap >= 0 else f"{gap:,}"
        gap_color = "#50C878" if gap >= 0 else ("#FFB400" if pct >= 70 else "#aaa")

        rcols = st.columns([0.4, 3.5, 1.2, 1.2, 1.2, 2.5, 1.0, 1.0])
        rcols[0].markdown(f"**{rank}**")

        link       = row.get("product_detail_link")
        title_text = row.get("display_title") or "-"
        thumb      = row.get("thumbnail_url")
        mpc        = row.get("mall_product_code") or ""
        img_tag = (
            f'<img src="{thumb}" style="width:36px;height:36px;object-fit:cover;'
            f'border-radius:4px;vertical-align:middle;margin-right:6px">'
            if thumb and isinstance(thumb, str) and thumb.startswith("http")
            else '<span style="display:inline-block;width:36px;height:36px;'
                 'background:#2a2f3e;border-radius:4px;vertical-align:middle;margin-right:6px"></span>'
        )
        mpc_tag = f'<div style="font-size:10px;color:#8899bb;margin-top:1px">{mpc}</div>' if mpc else ""
        if link and isinstance(link, str) and link.startswith("http"):
            rcols[1].markdown(
                f'{img_tag}<a href="{link}" target="_blank">{title_text}</a>{mpc_tag}',
                unsafe_allow_html=True,
            )
        else:
            rcols[1].markdown(f"{img_tag}{title_text}{mpc_tag}", unsafe_allow_html=True)

        rcols[2].markdown(fmt_won(row["gmv"]))
        rcols[3].markdown(f'<span style="color:{gap_color}">{gap_str}</span>',
                          unsafe_allow_html=True)
        rcols[4].markdown(f"**{pct:.1f}%**")
        rcols[5].markdown(progress_html(pct), unsafe_allow_html=True)
        rcols[6].markdown(row["eta"])

        if pct >= 100:
            rcols[7].markdown("✅ 달성")
        elif pct >= 70:
            rcols[7].markdown("⚡ 임박")
        elif row["gmv"] == 0:
            rcols[7].markdown("🚫 미판매")
        else:
            rcols[7].markdown("—")


# ── 섹션 렌더 ────────────────────────────────────────────────────────────────
st.divider()

# 정렬 토글
sort_col, _ = st.columns([2, 8])
with sort_col:
    is_eta_sort = st.session_state.sort_mode == "eta"
    if st.button(
        "⏱ ETA 짧은 순" if not is_eta_sort else "📊 GMV 높은 순으로 돌아가기",
        type="primary" if is_eta_sort else "secondary",
    ):
        st.session_state.sort_mode = "gmv" if is_eta_sort else "eta"
        st.rerun()

def sort_section(src: pd.DataFrame) -> pd.DataFrame:
    if st.session_state.sort_mode == "eta":
        # 달성 상품(eta_days=0) 제외하고 ETA 짧은 순, 데이터 없음(9999) 맨 뒤
        not_achieved = src[src["eta_days"] > 0].sort_values("eta_days")
        achieved     = src[src["eta_days"] == 0]
        return pd.concat([not_achieved, achieved]).reset_index(drop=True)
    return src.reset_index(drop=True)

if show_new:
    render_ranked_table(sort_section(filtered_new), "🆕 신상 랭킹")
if show_new and show_re and not filtered_new.empty:
    st.divider()
if show_re:
    render_ranked_table(sort_section(filtered_re), "🔁 재진행 랭킹")

# ── 꺾은선 차트: 주차별 GMV 트렌드 ──────────────────────────────────────────
st.divider()
st.markdown('<div class="section-header">📉 Top 20 상품 주차별 GMV 추이</div>', unsafe_allow_html=True)

top20 = df.head(20)
weekly_cols = ["gmv_w1", "gmv_w2", "gmv_w3", "gmv_w4_6", "gmv_w7_12"]
available_weekly = [c for c in weekly_cols if c in top20.columns]

if available_weekly:
    fig = go.Figure()
    week_labels = {"gmv_w1": "1주", "gmv_w2": "2주", "gmv_w3": "3주",
                   "gmv_w4_6": "4~6주", "gmv_w7_12": "7~12주"}
    x_labels = [week_labels.get(c, c) for c in available_weekly]

    for _, row in top20.iterrows():
        y = [row.get(c, 0) or 0 for c in available_weekly]
        if sum(y) == 0:
            continue
        fig.add_trace(go.Scatter(
            x=x_labels, y=y,
            mode="lines+markers",
            name=(row.get("display_title") or "")[:20],
            line=dict(width=1.5),
            marker=dict(size=4),
        ))

    fig.add_hline(
        y=THRESHOLD, line_dash="dot", line_color="#642FE9",
        annotation_text=f"임계값 {fmt_won(THRESHOLD)}",
        annotation_font_color="#642FE9",
    )
    fig.update_layout(
        height=380, margin=dict(l=0, r=0, t=20, b=0),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=11, color="#C8D4F0"),
        legend=dict(font=dict(size=9), orientation="v", x=1.01),
        xaxis=dict(gridcolor="#2a2f3e"),
        yaxis=dict(gridcolor="#2a2f3e", tickformat=","),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("주차별 GMV 컬럼(gmv_w1~gmv_w7_12)이 없습니다.")

# ── 임박 상품 요약 ────────────────────────────────────────────────────────────
near_df = df[(df["progress_pct"] >= 50) & ~df["is_hit"]].head(10)
if not near_df.empty:
    st.divider()
    st.markdown('<div class="section-header">⚡ 임계값 50% 이상 — 주목 상품</div>', unsafe_allow_html=True)
    tbl = near_df[["display_title", "gmv", "gap", "progress_pct", "eta", "gmv_last_7d"]].copy()
    tbl.columns = ["상품명", "누적 GMV", "임계값 대비", "달성률(%)", "달성 ETA", "최근 7일 GMV"]
    tbl["누적 GMV"] = tbl["누적 GMV"].apply(fmt_won)
    tbl["임계값 대비"] = tbl["임계값 대비"].apply(lambda v: f"+{v:,}" if v >= 0 else f"{v:,}")
    tbl["달성률(%)"] = tbl["달성률(%)"].apply(lambda v: f"{v:.1f}%")
    tbl["최근 7일 GMV"] = tbl["최근 7일 GMV"].apply(fmt_won)
    st.dataframe(tbl, use_container_width=True, hide_index=True)

# ── 자동 새로고침 ────────────────────────────────────────────────────────────
if auto_refresh and refresh_sec:
    time.sleep(refresh_sec)
    st.cache_data.clear()
    st.rerun()
