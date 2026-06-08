import streamlit as st
import streamlit.components.v1 as components
import requests
import re
import time
import json
import copy
import os
from datetime import datetime

# ── 빠른배송 아카이브 ─────────────────────────────────────────
ARCHIVED_CODES = [
    "ZE26SAC008","ZE26SAC011","ZE26SAC012","ZE26SAC900","ZE26SAC902",
    "ZE26SAC903","ZE26SAC904","ZE26SAC905","ZE26SAC906",
    "ZE26SBL010","ZE26SBL021","ZE26SBL901","ZE26SBL902","ZE26SBL904",
    "ZE26SBL905","ZE26SBL906","ZE26SBL907","ZE26SBL921",
    "ZE26SCA002","ZE26SCA012","ZE26SCA902","ZE26SCA903","ZE26SCA904",
    "ZE26SCA905","ZE26SCA907","ZE26SCA914",
    "ZE26SJK004","ZE26SJK902",
    "ZE26SOP001","ZE26SOP039","ZE26SOP904","ZE26SOP905","ZE26SOP906",
    "ZE26SOP907","ZE26SOP908","ZE26SOP909","ZE26SOP911","ZE26SOP912",
    "ZE26SOP915","ZE26SOP917","ZE26SOP936","ZE26SOP937",
    "ZE26SPT004","ZE26SPT005","ZE26SPT010","ZE26SPT012","ZE26SPT025",
    "ZE26SPT900","ZE26SPT901","ZE26SPT902","ZE26SPT920","ZE26SPT921",
    "ZE26SSH003","ZE26SSH901",
    "ZE26SSK013","ZE26SSK018","ZE26SSK029","ZE26SSK903","ZE26SSK904",
    "ZE26SSK905","ZE26SSK906","ZE26SSK907","ZE26SSK908","ZE26SSK910",
    "ZE26SSK926",
    "ZE26SST900",
    "ZE26SSU003","ZE26SSU005",
    "ZE26SSW006","ZE26SSW008","ZE26SSW015","ZE26SSW900","ZE26SSW902",
    "ZE26SSW903","ZE26SSW912",
    "ZE26STS007","ZE26STS900","ZE26STS901",
]

QUEENIT_PRODUCT_BASE = "https://web.queenit.kr/product/"

BASE_URL = "https://seller.api.queenit.kr"
ROLLBACK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# ── API 함수 ───────────────────────────────────────────────────
def parse_codes(text: str) -> list[str]:
    return [c.strip() for c in re.split(r"[\n,]+", text) if c.strip()]

def make_headers(t: str) -> dict:
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}

def get_proposal(code: str, headers: dict) -> dict:
    r = requests.get(f"{BASE_URL}/seller/product-proposals", params={"mallProductCode": code}, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def save_proposal(body: dict, headers: dict) -> dict:
    r = requests.put(f"{BASE_URL}/seller/product-proposals", json=body, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def submit_proposal(proposal_id: int, headers: dict) -> None:
    r = requests.post(f"{BASE_URL}/seller/product-proposals/{proposal_id}/submit", headers=headers, timeout=30)
    r.raise_for_status()

def fix_banner(html: str, new_url: str) -> str:
    tag = f'<img src="{new_url}" style="width:100%;display:block;" />'
    h = html or ""
    while h.startswith(tag):
        h = h[len(tag):]
    return tag + h

def strip_banner(html: str) -> tuple[str, bool]:
    h = html or ""
    new_h = re.sub(r'^(?:<img src="[^"]*" style="width:100%;display:block;" />)+', '', h)
    return new_h, new_h != h

def strip_prefix(title: str, prefix: str = "[빠른배송]") -> tuple[str, bool]:
    t = title or ""
    if t.startswith(prefix + " "):
        return t[len(prefix) + 1:], True
    if t.startswith(prefix):
        return t[len(prefix):], True
    return t, False

def fix_title(title: str, prefix: str) -> tuple[str, bool]:
    if not prefix:
        return title, False
    wrong = prefix + " "
    if title.startswith(wrong):
        return prefix + title[len(wrong):], True
    if title.startswith(prefix):
        return title, False
    return prefix + title, True

def load_rollback_files() -> list[dict]:
    """output/ 디렉토리의 rollback_*.json 파일 목록 반환 (최신순)"""
    if not os.path.exists(ROLLBACK_DIR):
        return []
    files = sorted(
        [f for f in os.listdir(ROLLBACK_DIR) if f.startswith("rollback_") and f.endswith(".json")],
        reverse=True,
    )
    result = []
    for fname in files:
        fpath = os.path.join(ROLLBACK_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                data["_filename"] = fname
                data["_filepath"] = fpath
                result.append(data)
        except Exception:
            continue
    return result

def build_put_body(data: dict, new_html: str) -> dict:
    body = {
        "productProposalId": data["productProposalId"],
        "mallProductCode": data["mallProductCode"],
        "mallId": data["mallId"],
        "brandCode": data["brandCode"],
        "title": data["title"],
        "price": data["price"],
        "imageUrls": data["imageUrls"],
        "itemProposals": data["itemProposals"],
        "optionTitles": data["optionTitles"],
        "descriptionPageHtml": new_html,
        "classifications": data.get("classifications") or [],
        "measurements": data.get("measurements") or [],
    }
    for f in ["announcement", "announcementV2", "salesStatus", "thumbnailLabel",
              "maxQuantityLimit", "maxQuantityLimitType", "isBundledProduct",
              "isBundleTargetProduct", "productIdsToBundle", "optionsCompositionInfo",
              "overrodePolicyTargetId", "isContestProduct", "isSample", "reifiedProductId"]:
        if data.get(f) is not None:
            body[f] = data[f]
    if data.get("categoryId"):
        body["leafCategoryId"] = data["categoryId"]
    return body

# ── 페이지 설정 ───────────────────────────────────────────────
st.set_page_config(
    page_title="퀸잇 상품 일괄 편집기",
    page_icon="🛍️",
    layout="wide",
)

st.markdown("""
<style>
/* ── 전역 폰트/크기 ── */
html, body, [class*="css"] {
    font-size: 13px !important;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

/* ── 배경 ── */
.stApp { background-color: #f5f5f7; }

/* ── 사이드바 배경 ── */
[data-testid="stSidebar"] {
    background-color: #121721 !important;
    border-right: none;
}
[data-testid="stSidebar"] * { color: #9b9da2 !important; }

/* ── 사이드바 상단 헤더 영역 ── */
[data-testid="stSidebarHeader"] {
    background-color: #121721 !important;
}

/* ── expander 섹션 카드 ── */
[data-testid="stSidebar"] [data-testid="stExpander"] {
    background-color: #1a2030 !important;
    border: 1px solid #2e3548 !important;
    border-radius: 8px !important;
    margin-bottom: 6px !important;
    transition: background 0.15s ease, border-color 0.15s ease;
}
[data-testid="stSidebar"] [data-testid="stExpander"]:hover {
    background-color: #5838bb !important;
    border-color: #5838bb !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"]:hover * {
    color: #ffffff !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"]:hover input,
[data-testid="stSidebar"] [data-testid="stExpander"]:hover textarea {
    background-color: rgba(255,255,255,0.15) !important;
    border-color: rgba(255,255,255,0.3) !important;
    color: #ffffff !important;
}

/* expander 헤더 (제목) — 내용 영역과 동일한 배경, 밝은 텍스트 */
[data-testid="stSidebar"] [data-testid="stExpander"] summary {
    background-color: #1a2030 !important;
    border-radius: 6px 6px 0 0 !important;
    padding: 8px 10px !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary,
[data-testid="stSidebar"] [data-testid="stExpander"] summary *,
[data-testid="stSidebar"] [data-testid="stExpander"] summary p {
    color: #c8d4f0 !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    letter-spacing: 0.03em;
}
[data-testid="stSidebar"] [data-testid="stExpanderToggleIcon"] svg {
    fill: #c8d4f0 !important;
}

/* expander 내부 위젯 */
[data-testid="stSidebar"] [data-testid="stExpander"] .stTextInput input,
[data-testid="stSidebar"] [data-testid="stExpander"] .stTextArea textarea {
    background-color: #121721 !important;
    border: 1px solid #2e3548 !important;
    color: #ffffff !important;
    font-size: 12px !important;
    border-radius: 6px !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] .stTextInput input::placeholder,
[data-testid="stSidebar"] [data-testid="stExpander"] .stTextArea textarea::placeholder {
    color: #4a5068 !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] .stTextInput input:focus,
[data-testid="stSidebar"] [data-testid="stExpander"] .stTextArea textarea:focus {
    border-color: #642FE9 !important;
    box-shadow: 0 0 0 2px rgba(100,47,233,0.2) !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] label {
    font-size: 11px !important;
    color: #9b9da2 !important;
    font-weight: 500;
}

/* 아카이브 섹션 링크 스타일 */
[data-testid="stSidebar"] .archive-link a {
    color: #a78bfa !important;
    text-decoration: none !important;
}
[data-testid="stSidebar"] .archive-link a:hover {
    color: #ffffff !important;
    text-decoration: underline !important;
}

/* ── 사이드바 닫혔을 때 열기 버튼 (stSidebarCollapsed) ── */
[data-testid="stSidebarCollapsed"] {
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
    z-index: 9999 !important;
    background-color: #121721 !important;
    border-right: 2px solid #2e3548 !important;
}
[data-testid="stSidebarCollapsed"] * {
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
}
[data-testid="stSidebarCollapsed"] svg {
    fill: #9b9da2 !important;
    stroke: #9b9da2 !important;
}
[data-testid="stSidebarCollapsed"]:hover {
    background-color: #1a2030 !important;
}
[data-testid="stSidebarCollapsed"]:hover svg {
    fill: #ffffff !important;
    stroke: #ffffff !important;
}
/* ── 사이드바 열기 버튼 (실제 선택자: stExpandSidebarButton) ── */
[data-testid="stExpandSidebarButton"] {
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
    z-index: 99999 !important;
    position: fixed !important;
    top: 0 !important;
    left: 0 !important;
    width: 2.5rem !important;
    height: 100vh !important;
    background-color: #121721 !important;
    border-right: 1px solid #2e3548 !important;
    align-items: center !important;
    justify-content: center !important;
    cursor: pointer !important;
}
[data-testid="stExpandSidebarButton"]:hover {
    background-color: #1a2030 !important;
}
[data-testid="stExpandSidebarButton"] button {
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
    background: transparent !important;
    border: none !important;
    cursor: pointer !important;
    padding: 0 !important;
}
[data-testid="stExpandSidebarButton"] svg {
    fill: #9b9da2 !important;
    width: 1.2rem !important;
    height: 1.2rem !important;
    display: block !important;
}
[data-testid="stExpandSidebarButton"]:hover svg {
    fill: #ffffff !important;
}
/* 사이드바 닫기 버튼 */
[data-testid="stSidebarCollapseButton"] svg {
    fill: #9b9da2 !important;
}

/* ── 메인 영역 ── */
h1 { font-size: 18px !important; font-weight: 700 !important; color: #121721 !important; }
h2 { font-size: 14px !important; font-weight: 600 !important; color: #121721 !important; }
h3 { font-size: 13px !important; font-weight: 600 !important; color: #3a3a6a !important; }

.stTextInput input, .stTextArea textarea {
    background-color: #ffffff !important;
    border: 1px solid #e0e0ec !important;
    border-radius: 6px !important;
    font-size: 12px !important;
    color: #1a1a2e !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: #642FE9 !important;
    box-shadow: 0 0 0 2px rgba(100,47,233,0.12) !important;
}
label[data-testid="stWidgetLabel"] {
    font-size: 12px !important; font-weight: 500 !important; color: #4a4a6a !important;
}

.stButton > button {
    background-color: #642FE9 !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 6px !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    padding: 6px 16px !important;
    transition: background 0.15s ease;
}
.stButton > button:hover { background-color: #5025cc !important; }
.stButton > button:disabled { background-color: #c0b0f0 !important; }

[data-testid="stProgressBar"] > div > div { background-color: #642FE9 !important; border-radius: 4px; }
[data-testid="stProgressBar"] { background-color: #e8e0fc !important; border-radius: 4px; }

[data-testid="stDataFrame"] { border: 1px solid #e0e0ec; border-radius: 8px; overflow: hidden; }
[data-testid="stDataFrame"] th {
    background-color: #f0ebfd !important; color: #642FE9 !important;
    font-size: 11px !important; font-weight: 600 !important;
    text-transform: uppercase; letter-spacing: 0.06em;
}
[data-testid="stDataFrame"] td { font-size: 12px !important; color: #2a2a4a !important; }

[data-testid="stInfo"] {
    background-color: #f0ebfd !important; border-left: 3px solid #642FE9 !important;
    border-radius: 6px !important; font-size: 12px !important; color: #3a1a99 !important;
}
[data-testid="stSuccess"] {
    background-color: #edfaf4 !important; border-left: 3px solid #00b87a !important;
    border-radius: 6px !important; font-size: 12px !important;
}
[data-testid="stWarning"] {
    background-color: #fff8ec !important; border-left: 3px solid #f5a623 !important;
    border-radius: 6px !important; font-size: 12px !important;
}

[data-testid="stCaptionContainer"] { font-size: 11px !important; color: #9898b8 !important; }
hr { border-color: #e0e0ec !important; }
/* stToolbar는 display:none 금지 — stExpandSidebarButton이 그 안에 있음 */
[data-testid="stToolbar"] { background: transparent !important; }
[data-testid="stToolbar"] > *:not([data-testid="stExpandSidebarButton"]) { display: none !important; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
.main .block-container {
    padding-top: 1.5rem !important;
    padding-bottom: 1.5rem !important;
    max-width: 960px;
}
</style>
""", unsafe_allow_html=True)

# ── 헤더 ──────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
  <div style="width:28px;height:28px;background:#f0ebfd;border-radius:7px;
              display:flex;align-items:center;justify-content:center;font-size:15px;">🛍️</div>
  <div>
    <div style="font-size:17px;font-weight:700;color:#121721;line-height:1.2;">퀸잇 상품 일괄 편집기</div>
    <div style="font-size:11px;color:#9898b8;">배너 이미지 추가 · 상품명 프리픽스 설정을 한 번에 처리합니다</div>
  </div>
</div>
<div style="height:1px;background:#e0e0ec;margin:12px 0 16px;"></div>
""", unsafe_allow_html=True)

# ── 사이드바 닫혔을 때 열기 버튼 ─────────────────────────────
# st.markdown은 <script>를 제거하므로 div만 주입, 스크립트는 components.html로 분리
st.markdown("""
<div id="custom-sidebar-btn" style="
    display:none;
    position:fixed;
    left:0; top:0;
    width:28px; height:100vh;
    background:#121721;
    border-right:1px solid #2e3548;
    z-index:999999;
    cursor:pointer;
    align-items:center;
    justify-content:center;
">
  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="#9b9da2">
    <path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"/>
  </svg>
</div>
""", unsafe_allow_html=True)

# components.html은 iframe 안에서 실행 → window.parent.document로 부모 DOM 접근
components.html("""
<script>
(function() {
  var pd = window.parent.document;

  function updateBtn() {
    var sidebar = pd.querySelector('[data-testid="stSidebar"]');
    var btn = pd.getElementById('custom-sidebar-btn');
    if (!sidebar || !btn) return;
    var isCollapsed = sidebar.getAttribute('aria-expanded') === 'false'
      || sidebar.getBoundingClientRect().width < 10;
    btn.style.display = isCollapsed ? 'flex' : 'none';
  }

  function setupClick() {
    var btn = pd.getElementById('custom-sidebar-btn');
    if (btn && !btn._clickSetup) {
      btn._clickSetup = true;
      btn.addEventListener('click', function() {
        var expandBtn = pd.querySelector('[data-testid="stExpandSidebarButton"]');
        if (expandBtn) {
          var inner = expandBtn.querySelector('button') || expandBtn;
          inner.click();
        }
      });
    }
  }

  function init() {
    var sidebar = pd.querySelector('[data-testid="stSidebar"]');
    if (sidebar) {
      var observer = new MutationObserver(updateBtn);
      observer.observe(sidebar, {
        attributes: true,
        attributeFilter: ['aria-expanded', 'style', 'class']
      });
      updateBtn();
      setupClick();
    } else {
      setTimeout(init, 300);
    }
  }

  setTimeout(init, 600);
  setInterval(function() { updateBtn(); setupClick(); }, 500);
})();
</script>
""", height=0)

# ── 사이드바 ──────────────────────────────────────────────────
with st.sidebar:
    with st.expander("⚙ 설정", expanded=True):
        token = st.text_input(
            "Seller Token",
            type="password",
            placeholder="Bearer 토큰을 입력하세요",
            help="Seller Admin → 개발자도구 Network → Authorization: Bearer {값}",
        )

    with st.expander("🖼 배너 이미지", expanded=True):
        banner_url = st.text_input(
            "배너 이미지 URL",
            placeholder="https://image.queenit.kr/...",
        )

    with st.expander("🏷 상품명 프리픽스", expanded=True):
        use_prefix = st.checkbox("상품명 프리픽스 추가", value=False)
        title_prefix = ""
        if use_prefix:
            title_prefix = st.text_input("프리픽스 문자열", value="[빠른배송]", placeholder="예: [빠른배송]")

    with st.expander("🔧 실행 설정", expanded=True):
        dry_run = st.toggle("Dry-run (미리보기)", value=True, help="실제 변경 없이 조회만 수행")
        delay = st.slider("요청 간 딜레이 (초)", 0.2, 2.0, 0.5, 0.1)

    with st.expander("🔄 롤백", expanded=False):
        _rb_files = load_rollback_files()
        _snap = st.session_state.get("last_snapshot")
        # 현재 세션 스냅샷(파일 미저장) + 과거 JSON 파일 병합
        _rb_options = []
        if _snap and not _snap.get("_filepath"):
            _rb_options.append(_snap)
        for _rf in _rb_files:
            _rb_options.append(_rf)

        if _rb_options:
            _labels = []
            for _o in _rb_options:
                _n = len(_o.get("entries", []))
                _t = _o.get("timestamp", "")
                _src = " (현재 세션)" if not _o.get("_filepath") else ""
                _labels.append(f"{_t}{_src} · {_n}개")
            _sel_idx = st.selectbox(
                "복원할 작업 선택",
                range(len(_rb_options)),
                format_func=lambda i: _labels[i],
                key="rb_select",
                label_visibility="collapsed",
            )
            _sel = _rb_options[_sel_idx]
            st.markdown(
                f'<div style="font-size:11px;color:#9b9da2;margin-bottom:6px;">'
                f'{len(_sel.get("entries", []))}개 상품 복원 가능</div>',
                unsafe_allow_html=True,
            )
            if st.button("↩ 롤백 실행", key="goto_rollback", use_container_width=True):
                st.session_state["selected_rollback"] = _sel
                st.session_state["view"] = "rollback"
                st.rerun()
        else:
            st.markdown(
                '<div style="font-size:11px;color:#9b9da2;">실행 이력이 없습니다.</div>',
                unsafe_allow_html=True,
            )

    with st.expander("📋 작업 아카이브", expanded=False):
        st.markdown(
            f'<div style="font-size:11px;color:#9b9da2;margin-bottom:8px;">'
            f'[빠른배송] 적용 상품 {len(ARCHIVED_CODES)}개</div>',
            unsafe_allow_html=True,
        )
        if st.button("상품 조회", key="view_archive", use_container_width=True):
            if not token:
                st.warning("⚙ 설정에서 Seller Token을 먼저 입력하세요.")
            else:
                h = make_headers(token)
                names, urls = {}, {}
                prog = st.progress(0)
                for i, code in enumerate(ARCHIVED_CODES):
                    try:
                        data = get_proposal(code, h)["productProposal"]["data"]
                        names[code] = data.get("title", "")
                        pid = data.get("reifiedProductId") or data.get("itemId") or ""
                        urls[code] = f"{QUEENIT_PRODUCT_BASE}{pid}?openBy=sellerAdmin" if pid else ""
                    except Exception:
                        names[code] = ""
                        urls[code] = ""
                    prog.progress((i + 1) / len(ARCHIVED_CODES))
                prog.empty()
                st.session_state["archive_names"] = names
                st.session_state["archive_urls"] = urls
                st.session_state["view"] = "archive"
                st.rerun()
        if st.button("↩ 원복 실행", key="goto_revert", use_container_width=True):
            st.session_state["view"] = "revert"
            st.rerun()

# ── 메인 ─────────────────────────────────────────────────────
if st.session_state.get("view") == "archive":
    col_title, col_back = st.columns([3, 1])
    with col_title:
        st.markdown(
            '<div style="font-size:15px;font-weight:700;color:#121721;margin-bottom:4px;">'
            '📋 작업 아카이브 · [빠른배송] 적용 상품</div>',
            unsafe_allow_html=True,
        )
    with col_back:
        if st.button("← 일괄 편집으로", key="back_to_edit"):
            st.session_state["view"] = "edit"
            st.rerun()

    archive_names = st.session_state.get("archive_names", {})
    archive_urls  = st.session_state.get("archive_urls", {})

    rows_html = ""
    for i, code in enumerate(ARCHIVED_CODES):
        name = archive_names.get(code, "")
        url  = archive_urls.get(code, "")
        bg   = "#ffffff" if i % 2 == 0 else "#faf9fd"
        code_cell = (
            f'<a href="{url}" target="_blank" '
            f'style="color:#642FE9;font-weight:600;text-decoration:none;">{code} ↗</a>'
            if url else
            f'<span style="color:#642FE9;font-weight:600;">{code}</span>'
        )
        detail = f"변경된 상품명: {name}" if name else "-"
        rows_html += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:8px 12px;border-bottom:1px solid #e0e0ec;white-space:nowrap;">{code_cell}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #e0e0ec;color:#4a4a6a;font-size:12px;">배너 이미지 추가 · [빠른배송] 프리픽스 추가</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #e0e0ec;color:#2a2a4a;font-size:12px;">{detail}</td>'
            f'</tr>'
        )

    st.markdown(
        f'<div style="border:1px solid #e0e0ec;border-radius:8px;overflow:hidden;">'
        f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
        f'<thead><tr style="background:#f0ebfd;">'
        f'<th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:600;color:#642FE9;letter-spacing:0.05em;white-space:nowrap;">품번</th>'
        f'<th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:600;color:#642FE9;letter-spacing:0.05em;">작업 내용</th>'
        f'<th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:600;color:#642FE9;letter-spacing:0.05em;">작업 내용 상세</th>'
        f'</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table></div>',
        unsafe_allow_html=True,
    )

elif st.session_state.get("view") == "rollback":
    snap = st.session_state.get("selected_rollback") or st.session_state.get("last_snapshot") or {}
    entries = snap.get("entries", [])
    ts = snap.get("timestamp", "")
    snap_src = snap.get("_filename", "현재 세션")

    col_title, col_back = st.columns([3, 1])
    with col_title:
        st.markdown(
            f'<div style="font-size:15px;font-weight:700;color:#121721;margin-bottom:4px;">'
            f'⏮ 롤백 — 이전 상태 복원</div>'
            f'<div style="font-size:11px;color:#9898b8;margin-bottom:12px;">'
            f'원본 작업: {ts} · {len(entries)}개 상품 · 출처: {snap_src}</div>',
            unsafe_allow_html=True,
        )
    with col_back:
        if st.button("← 일괄 편집으로", key="rollback_back"):
            st.session_state["view"] = "edit"
            st.rerun()

    if not entries:
        st.info("복원할 이력이 없습니다.")
    else:
        rows_html = ""
        for i, e in enumerate(entries):
            bg = "#ffffff" if i % 2 == 0 else "#faf9fd"
            rows_html += (
                f'<tr style="background:{bg};">'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e0e0ec;white-space:nowrap;'
                f'color:#642FE9;font-weight:600;">{e["code"]}</td>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e0e0ec;color:#2a2a4a;font-size:12px;">'
                f'{e["original_title"] or "-"}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<div style="border:1px solid #e0e0ec;border-radius:8px;overflow:hidden;margin-bottom:16px;">'
            f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
            f'<thead><tr style="background:#fff8ec;">'
            f'<th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:600;color:#f5a623;'
            f'letter-spacing:0.05em;white-space:nowrap;">품번</th>'
            f'<th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:600;color:#f5a623;'
            f'letter-spacing:0.05em;">복원될 상품명</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div>',
            unsafe_allow_html=True,
        )

        st.warning(f"위 {len(entries)}개 상품을 원본 상태로 복원합니다. 이 작업은 되돌릴 수 없습니다.")

        if not token:
            st.info("⚙ 설정에서 Seller Token을 먼저 입력하세요.")
        else:
            if st.button("⏮ 롤백 실행", type="primary", key="do_rollback"):
                headers = make_headers(token)
                rb_progress = st.progress(0)
                rb_status = st.empty()
                rb_results = []
                for i, e in enumerate(entries):
                    rb_status.markdown(
                        f'<div style="font-size:12px;color:#f5a623;margin:4px 0;">'
                        f'복원 중… <b>{e["code"]}</b> ({i+1}/{len(entries)})</div>',
                        unsafe_allow_html=True,
                    )
                    try:
                        rd = e["rollback_data"]
                        rd["title"] = e["original_title"]
                        put_body = build_put_body(rd, e["original_html"])
                        save_result = save_proposal(put_body, headers)
                        submit_proposal(
                            save_result["productProposal"]["data"]["productProposalId"],
                            headers,
                        )
                        rb_results.append({"코드": e["code"], "상태": "✅ 복원 완료"})
                    except requests.HTTPError as ex:
                        rb_results.append({"코드": e["code"], "상태": f"❌ HTTP {ex.response.status_code}"})
                    except Exception as ex:
                        rb_results.append({"코드": e["code"], "상태": f"❌ {str(ex)[:60]}"})
                    rb_progress.progress((i + 1) / len(entries))
                    time.sleep(0.3)

                rb_status.empty()
                ok = sum(1 for r in rb_results if "✅" in r["상태"])
                st.dataframe(rb_results, use_container_width=True, hide_index=True)
                if ok == len(entries):
                    st.success(f"롤백 완료! {ok}개 상품이 원본으로 복원되었습니다.")
                    # 파일 기반 롤백이면 JSON 파일 삭제
                    _fp = snap.get("_filepath")
                    if _fp and os.path.exists(_fp):
                        os.remove(_fp)
                    st.session_state["last_snapshot"] = None
                    st.session_state["selected_rollback"] = None
                else:
                    st.warning(f"롤백: {ok}개 성공 / {len(entries)-ok}개 실패")

elif st.session_state.get("view") == "revert":
    # ── 원복 뷰 ──────────────────────────────────────────────
    col_title, col_back = st.columns([3, 1])
    with col_title:
        st.markdown(
            '<div style="font-size:15px;font-weight:700;color:#121721;margin-bottom:4px;">'
            '↩ 원복 — [빠른배송] 적용 해제</div>'
            f'<div style="font-size:11px;color:#9898b8;margin-bottom:12px;">'
            f'대상: {len(ARCHIVED_CODES)}개 상품 · [빠른배송] 프리픽스 및 배너 이미지 제거</div>',
            unsafe_allow_html=True,
        )
    with col_back:
        if st.button("← 일괄 편집으로", key="revert_back"):
            st.session_state["view"] = "edit"
            st.rerun()

    st.info("이미 프리픽스·배너가 제거된 상품은 자동으로 건너뜁니다.")

    if not token:
        st.info("⚙ 설정에서 Seller Token을 먼저 입력하세요.")
    else:
        if dry_run:
            st.warning("현재 Dry-run 모드입니다. 실제 변경이 적용되지 않습니다.")
        else:
            st.warning(f"⚠️ {len(ARCHIVED_CODES)}개 상품의 [빠른배송] 프리픽스와 배너 이미지를 제거합니다.")

        mode_label = "DRY-RUN" if dry_run else "실제 실행"
        if st.button(f"↩ 원복 실행 ({mode_label})", type="primary", key="do_revert"):
            headers = make_headers(token)
            total = len(ARCHIVED_CODES)
            rv_results = []
            rv_progress = st.progress(0)
            rv_status = st.empty()
            rv_table = st.empty()

            for i, code in enumerate(ARCHIVED_CODES):
                rv_status.markdown(
                    f'<div style="font-size:12px;color:#642FE9;margin:4px 0;">'
                    f'처리 중… <b>{code}</b> ({i+1}/{total})</div>',
                    unsafe_allow_html=True,
                )
                try:
                    resp = get_proposal(code, headers)
                    data = resp["productProposal"]["data"]
                    current_html = data.get("descriptionPageHtml") or ""
                    current_title = data.get("title", "")

                    new_html, banner_removed = strip_banner(current_html)
                    new_title, prefix_removed = strip_prefix(current_title, "[빠른배송]")

                    if not banner_removed and not prefix_removed:
                        rv_results.append({
                            "코드": code,
                            "상품명": current_title,
                            "변경사항": "이미 제거됨",
                            "상태": "⏭ 건너뜀",
                        })
                    else:
                        changes = []
                        if banner_removed:
                            changes.append("배너 제거")
                        if prefix_removed:
                            changes.append("프리픽스 제거")

                        if not dry_run:
                            data["title"] = new_title
                            put_body = build_put_body(data, new_html)
                            save_result = save_proposal(put_body, headers)
                            submit_proposal(
                                save_result["productProposal"]["data"]["productProposalId"],
                                headers,
                            )

                        rv_results.append({
                            "코드": code,
                            "상품명": f"{current_title} → {new_title}" if prefix_removed else current_title,
                            "변경사항": ", ".join(changes),
                            "상태": "✅ 완료" if not dry_run else "🔍 확인",
                        })

                except requests.HTTPError as ex:
                    rv_results.append({"코드": code, "상품명": "-", "변경사항": "-", "상태": f"❌ HTTP {ex.response.status_code}"})
                except Exception as ex:
                    rv_results.append({"코드": code, "상품명": "-", "변경사항": "-", "상태": f"❌ {str(ex)[:60]}"})

                rv_progress.progress((i + 1) / total)
                rv_table.dataframe(rv_results, use_container_width=True, hide_index=True)
                time.sleep(delay)

            rv_status.empty()
            ok = sum(1 for r in rv_results if "✅" in r["상태"])
            skipped = sum(1 for r in rv_results if "⏭" in r["상태"])
            fail = sum(1 for r in rv_results if "❌" in r["상태"])

            if not dry_run:
                st.success(f"원복 완료! {ok}개 성공 · {skipped}개 건너뜀 · {fail}개 실패")
            else:
                st.info(f"미리보기: {ok}개 변경 예정 · {skipped}개 이미 제거됨 · {fail}개 오류")

else:
    tab_edit, tab_banner_remove = st.tabs(["✏️ 일괄 편집", "🗑 배너 제거"])

    # ── 일괄 편집 탭 ─────────────────────────────────────────
    with tab_edit:
        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.markdown("**대상 상품 코드**")
            codes_input = st.text_area(
                "상품 코드",
                height=160,
                placeholder="ZE26SAC008\nZE26SAC011\nZE26SAC012\n...\n\n한 줄에 하나씩 또는 쉼표로 구분",
                label_visibility="collapsed",
            )

        codes = parse_codes(codes_input)

        with col_right:
            st.markdown("**요약**")
            badge_banner = '<span style="background:#f0ebfd;color:#642FE9;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600;margin-right:4px;">배너</span>' if banner_url else ""
            badge_prefix = '<span style="background:#f0ebfd;color:#642FE9;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600;">프리픽스</span>' if (use_prefix and title_prefix) else ""
            badge_none = '<span style="color:#c0c0d0;font-size:11px;">미설정</span>' if (not banner_url and not (use_prefix and title_prefix)) else ""
            badge_mode = '<span style="background:#fff8ec;color:#f5a623;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600;">DRY-RUN</span>' if dry_run else '<span style="background:#edfaf4;color:#00b87a;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600;">실제 실행</span>'
            st.markdown(f"""
            <div style="background:#fff;border:1px solid #e0e0ec;border-radius:8px;padding:14px;font-size:12px;">
              <div style="color:#9898b8;margin-bottom:6px;">상품 수</div>
              <div style="font-size:22px;font-weight:700;color:#642FE9;">{len(codes)}<span style="font-size:13px;font-weight:400;color:#9898b8;">개</span></div>
              <div style="margin-top:10px;color:#9898b8;">작업</div>
              <div style="margin-top:3px;">{badge_banner}{badge_prefix}{badge_none}</div>
              <div style="margin-top:10px;color:#9898b8;">모드</div>
              <div style="margin-top:3px;">{badge_mode}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

        can_run = bool(token and codes and (banner_url or (use_prefix and title_prefix)))
        if not can_run:
            missing = []
            if not token: missing.append("Seller Token")
            if not codes: missing.append("상품 코드")
            if not banner_url and not (use_prefix and title_prefix):
                missing.append("배너 URL 또는 프리픽스")
            if missing:
                st.info(f"실행 전 필요: {', '.join(missing)}")

        run_label = "🔍 미리보기 실행" if dry_run else "🚀 실제 실행"
        run_btn = st.button(run_label, type="primary", disabled=not can_run)

        if run_btn:
            headers = make_headers(token)
            total = len(codes)
            results = []
            snapshot_entries = []

            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
            st.markdown("**진행 현황**")
            progress_bar = st.progress(0)
            status_text = st.empty()
            result_table = st.empty()

            for i, code in enumerate(codes):
                status_text.markdown(
                    f'<div style="font-size:12px;color:#642FE9;margin:4px 0;">처리 중… <b>{code}</b> ({i+1}/{total})</div>',
                    unsafe_allow_html=True,
                )
                try:
                    resp = get_proposal(code, headers)
                    data = resp["productProposal"]["data"]
                    current_html = data.get("descriptionPageHtml") or ""
                    current_title = data.get("title", "")

                    original_data = copy.deepcopy(data)
                    original_html = current_html

                    changes = []
                    new_html = current_html
                    new_title = current_title

                    if banner_url:
                        new_html = fix_banner(current_html, banner_url)
                        changes.append("배너 ✅")

                    if use_prefix and title_prefix:
                        new_title, changed = fix_title(current_title, title_prefix)
                        data["title"] = new_title
                        if changed:
                            changes.append("상품명 ✅")

                    if not dry_run:
                        put_body = build_put_body(data, new_html)
                        save_result = save_proposal(put_body, headers)
                        submit_proposal(save_result["productProposal"]["data"]["productProposalId"], headers)
                        if changes:
                            snapshot_entries.append({
                                "code": code,
                                "original_title": current_title,
                                "original_html": original_html,
                                "rollback_data": original_data,
                            })

                    results.append({
                        "코드": code,
                        "상품명": f"{current_title} → {new_title}" if new_title != current_title else current_title,
                        "변경사항": ", ".join(changes) if changes else "-",
                        "상태": "✅ 완료" if not dry_run else "🔍 확인",
                    })

                except requests.HTTPError as e:
                    results.append({"코드": code, "상품명": "-", "변경사항": "-", "상태": f"❌ HTTP {e.response.status_code}"})
                except Exception as e:
                    results.append({"코드": code, "상품명": "-", "변경사항": "-", "상태": f"❌ {str(e)[:60]}"})

                progress_bar.progress((i + 1) / total)
                result_table.dataframe(results, use_container_width=True, hide_index=True)

                if i < total - 1:
                    time.sleep(delay)

            status_text.empty()
            success = sum(1 for r in results if "❌" not in r["상태"])
            fail = total - success

            if dry_run:
                st.success(f"미리보기 완료: {total}개 상품 조회 성공")
                st.info("실제 반영하려면 사이드바에서 Dry-run 토글을 끄고 다시 실행하세요.")
            else:
                if fail == 0:
                    st.success(f"완료! {success}개 성공 / {fail}개 실패")
                else:
                    st.warning(f"완료: {success}개 성공 / {fail}개 실패")

                if snapshot_entries:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    snapshot = {"timestamp": ts, "entries": snapshot_entries}
                    st.session_state["last_snapshot"] = snapshot
                    os.makedirs(ROLLBACK_DIR, exist_ok=True)
                    fname = f"rollback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(os.path.join(ROLLBACK_DIR, fname), "w", encoding="utf-8") as fp:
                        json.dump(snapshot, fp, ensure_ascii=False, indent=2)
                    st.markdown(
                        f'<div style="margin-top:12px;padding:10px 14px;background:#fff8ec;'
                        f'border:1px solid #f5a623;border-radius:8px;font-size:12px;color:#7a4c00;">'
                        f'⏮ 작업을 되돌리려면 사이드바 <b>🔄 롤백</b> 섹션을 이용하세요.</div>',
                        unsafe_allow_html=True,
                    )

    # ── 배너 제거 탭 ─────────────────────────────────────────
    with tab_banner_remove:
        col_left_br, col_right_br = st.columns([2, 1])

        with col_left_br:
            st.markdown("**대상 상품 코드**")
            br_codes_input = st.text_area(
                "배너 제거 대상 코드",
                height=160,
                placeholder="ZE26SAC008\nZE26SAC011\nZE26SAC012\n...\n\n한 줄에 하나씩 또는 쉼표로 구분",
                label_visibility="collapsed",
                key="br_codes_input",
            )

        br_codes = parse_codes(br_codes_input)

        with col_right_br:
            st.markdown("**요약**")
            badge_mode_br = (
                '<span style="background:#fff8ec;color:#f5a623;padding:2px 7px;border-radius:4px;'
                'font-size:11px;font-weight:600;">DRY-RUN</span>'
                if dry_run else
                '<span style="background:#edfaf4;color:#00b87a;padding:2px 7px;border-radius:4px;'
                'font-size:11px;font-weight:600;">실제 실행</span>'
            )
            st.markdown(f"""
            <div style="background:#fff;border:1px solid #e0e0ec;border-radius:8px;padding:14px;font-size:12px;">
              <div style="color:#9898b8;margin-bottom:6px;">상품 수</div>
              <div style="font-size:22px;font-weight:700;color:#e53e3e;">{len(br_codes)}<span style="font-size:13px;font-weight:400;color:#9898b8;">개</span></div>
              <div style="margin-top:10px;color:#9898b8;">작업</div>
              <div style="margin-top:3px;">
                <span style="background:#fef2f2;color:#e53e3e;padding:2px 7px;border-radius:4px;
                font-size:11px;font-weight:600;">배너 제거</span>
              </div>
              <div style="margin-top:10px;color:#9898b8;">모드</div>
              <div style="margin-top:3px;">{badge_mode_br}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

        can_run_br = bool(token and br_codes)
        if not can_run_br:
            missing_br = []
            if not token: missing_br.append("Seller Token")
            if not br_codes: missing_br.append("상품 코드")
            if missing_br:
                st.info(f"실행 전 필요: {', '.join(missing_br)}")

        if dry_run:
            st.warning("현재 Dry-run 모드입니다. 실제 변경이 적용되지 않습니다.")

        br_run_label = "🔍 미리보기" if dry_run else "🗑 배너 제거 실행"
        br_run_btn = st.button(br_run_label, type="primary", disabled=not can_run_br, key="br_run_btn")

        if br_run_btn:
            headers = make_headers(token)
            total_br = len(br_codes)
            br_results = []
            br_snapshot_entries = []

            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
            st.markdown("**진행 현황**")
            br_progress = st.progress(0)
            br_status = st.empty()
            br_table = st.empty()

            for i, code in enumerate(br_codes):
                br_status.markdown(
                    f'<div style="font-size:12px;color:#e53e3e;margin:4px 0;">'
                    f'처리 중… <b>{code}</b> ({i+1}/{total_br})</div>',
                    unsafe_allow_html=True,
                )
                try:
                    resp = get_proposal(code, headers)
                    data = resp["productProposal"]["data"]
                    current_html = data.get("descriptionPageHtml") or ""
                    current_title = data.get("title", "")

                    new_html, banner_removed = strip_banner(current_html)

                    if not banner_removed:
                        br_results.append({
                            "코드": code,
                            "상품명": current_title,
                            "변경사항": "배너 없음",
                            "상태": "⏭ 건너뜀",
                        })
                    else:
                        if not dry_run:
                            original_data = copy.deepcopy(data)
                            put_body = build_put_body(data, new_html)
                            save_result = save_proposal(put_body, headers)
                            submit_proposal(
                                save_result["productProposal"]["data"]["productProposalId"],
                                headers,
                            )
                            br_snapshot_entries.append({
                                "code": code,
                                "original_title": current_title,
                                "original_html": current_html,
                                "rollback_data": original_data,
                            })
                        br_results.append({
                            "코드": code,
                            "상품명": current_title,
                            "변경사항": "배너 제거",
                            "상태": "✅ 완료" if not dry_run else "🔍 확인",
                        })

                except requests.HTTPError as ex:
                    br_results.append({"코드": code, "상품명": "-", "변경사항": "-", "상태": f"❌ HTTP {ex.response.status_code}"})
                except Exception as ex:
                    br_results.append({"코드": code, "상품명": "-", "변경사항": "-", "상태": f"❌ {str(ex)[:60]}"})

                br_progress.progress((i + 1) / total_br)
                br_table.dataframe(br_results, use_container_width=True, hide_index=True)
                if i < total_br - 1:
                    time.sleep(delay)

            br_status.empty()
            ok_br = sum(1 for r in br_results if "✅" in r["상태"])
            skip_br = sum(1 for r in br_results if "⏭" in r["상태"])
            fail_br = sum(1 for r in br_results if "❌" in r["상태"])

            if dry_run:
                st.info(f"미리보기: {ok_br}개 배너 제거 예정 · {skip_br}개 배너 없음 · {fail_br}개 오류")
            else:
                if fail_br == 0:
                    st.success(f"완료! {ok_br}개 성공 · {skip_br}개 건너뜀 · {fail_br}개 실패")
                else:
                    st.warning(f"완료: {ok_br}개 성공 · {skip_br}개 건너뜀 · {fail_br}개 실패")

                if br_snapshot_entries:
                    ts_br = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    snapshot_br = {"timestamp": ts_br, "entries": br_snapshot_entries}
                    st.session_state["last_snapshot"] = snapshot_br
                    os.makedirs(ROLLBACK_DIR, exist_ok=True)
                    fname_br = f"rollback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(os.path.join(ROLLBACK_DIR, fname_br), "w", encoding="utf-8") as fp:
                        json.dump(snapshot_br, fp, ensure_ascii=False, indent=2)
                    st.markdown(
                        f'<div style="margin-top:12px;padding:10px 14px;background:#fff8ec;'
                        f'border:1px solid #f5a623;border-radius:8px;font-size:12px;color:#7a4c00;">'
                        f'⏮ 작업을 되돌리려면 사이드바 <b>🔄 롤백</b> 섹션을 이용하세요.</div>',
                        unsafe_allow_html=True,
                    )
