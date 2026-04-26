"""
quant_system/utils/ui_auth.py — Streamlit UI 身份驗證模組（① Phase G）

設計原則：
  - 零外部依賴（不需要 streamlit-authenticator）
  - 密碼以 PBKDF2-HMAC-SHA256 雜湊儲存，不保存明文
  - 憑證來源優先順序：
      1. .streamlit/secrets.toml  （推薦：Streamlit Cloud 與本機共用）
      2. 環境變數 UI_USERNAME / UI_PASSWORD_HASH
      3. 環境變數 UI_USERNAME / UI_PASSWORD（自動雜湊，僅開發用）
  - 登入狀態以 `st.session_state["_aqt_authenticated"]` 管理
  - 密碼雜湊生成工具：`python -m quant_system.utils.ui_auth hash`

憑證設定範例（.streamlit/secrets.toml）：
  [auth]
  username      = "admin"
  password_hash = "pbkdf2:sha256:600000$..."   # 由 hash 指令產生

憑證設定範例（.env）：
  UI_USERNAME=admin
  UI_PASSWORD_HASH=pbkdf2:sha256:600000$...

安全注意事項：
  - 不要將 .streamlit/secrets.toml 提交至版本控制（已加入 .gitignore 建議）
  - secrets.toml 的 [auth] password_hash 欄位儲存的是雜湊，即使洩漏也無法還原原始密碼
  - 本模組不提供「記住我」功能，每次重啟 Streamlit 都需要重新登入
  - 若 UI 部署於公網，強烈建議搭配 HTTPS（nginx + Let's Encrypt）
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import time
from typing import Optional

log = logging.getLogger(__name__)

# ── 雜湊參數 ──────────────────────────────────────────────────
_HASH_ALGO      = "sha256"
_ITERATIONS     = 600_000   # OWASP 2023 建議最低值
_HASH_PREFIX    = f"pbkdf2:{_HASH_ALGO}:{_ITERATIONS}"

# ── 暴力破解防護 ───────────────────────────────────────────────
_MAX_ATTEMPTS   = 5         # 連續失敗 N 次 → 鎖定
_LOCKOUT_SEC    = 300       # 鎖定 5 分鐘


def hash_password(plain: str) -> str:
    """
    使用 PBKDF2-HMAC-SHA256 雜湊密碼。

    格式：pbkdf2:sha256:600000$<salt_hex>$<hash_hex>
    可直接存入 secrets.toml 或環境變數。

    Args:
        plain — 明文密碼

    Returns:
        可安全儲存的雜湊字串
    """
    salt    = secrets.token_hex(16)
    dk      = hashlib.pbkdf2_hmac(
        _HASH_ALGO,
        plain.encode("utf-8"),
        salt.encode("utf-8"),
        _ITERATIONS,
    )
    return f"{_HASH_PREFIX}${salt}${dk.hex()}"


def verify_password(plain: str, stored_hash: str) -> bool:
    """
    驗證明文密碼是否符合儲存的雜湊。
    使用 hmac.compare_digest 防止 timing attack。

    Args:
        plain       — 使用者輸入的明文密碼
        stored_hash — hash_password() 回傳的字串

    Returns:
        True = 密碼正確
    """
    try:
        parts = stored_hash.split("$")
        if len(parts) != 3:
            return False
        # 拆解 prefix$salt$hash
        prefix, salt, expected_hex = parts
        # 驗證前綴格式
        prefix_parts = prefix.split(":")
        if len(prefix_parts) != 3 or prefix_parts[0] != "pbkdf2":
            return False
        algo       = prefix_parts[1]
        iterations = int(prefix_parts[2])

        dk = hashlib.pbkdf2_hmac(
            algo,
            plain.encode("utf-8"),
            salt.encode("utf-8"),
            iterations,
        )
        return hmac.compare_digest(dk.hex(), expected_hex)
    except Exception:
        return False


def _load_credentials() -> tuple[Optional[str], Optional[str]]:
    """
    依優先順序載入憑證。
    回傳 (username, password_hash)，未設定時回傳 (None, None)。
    """
    # 1. .streamlit/secrets.toml
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "auth" in st.secrets:
            uname = st.secrets["auth"].get("username", "")
            phash = st.secrets["auth"].get("password_hash", "")
            if uname and phash:
                return uname, phash
    except Exception:
        pass

    # 2. 環境變數 UI_PASSWORD_HASH（推薦）
    uname = os.getenv("UI_USERNAME", "")
    phash = os.getenv("UI_PASSWORD_HASH", "")
    if uname and phash:
        return uname, phash

    # 3. 環境變數 UI_PASSWORD（明文，開發用）→ 自動雜湊（不持久化）
    plain = os.getenv("UI_PASSWORD", "")
    if uname and plain:
        log.warning(
            "[Auth] 使用明文 UI_PASSWORD（僅適合開發環境）。"
            "正式環境請改用 UI_PASSWORD_HASH。"
        )
        return uname, hash_password(plain)

    return None, None


def require_login(title: str = "AQT 量化交易系統") -> bool:
    """
    ① Streamlit 登入守門員。

    在需要保護的頁面最頂端呼叫此函式。
    - 若使用者已登入 → 回傳 True，繼續渲染頁面
    - 若未登入 → 顯示登入表單，停止後續渲染，回傳 False

    使用範例（test.py 開頭）：
        from quant_system.utils.ui_auth import require_login
        if not require_login():
            st.stop()
        # ... 以下為受保護的 UI 內容 ...

    若未設定任何憑證（UI_USERNAME 等），直接放行（方便本機開發）。
    """
    import streamlit as st

    username, password_hash = _load_credentials()

    # 未設定憑證 → 放行（本機開發模式）
    if not username or not password_hash:
        log.debug("[Auth] 未設定 UI 憑證，跳過身份驗證（開發模式）")
        return True

    # 已登入
    if st.session_state.get("_aqt_authenticated", False):
        # 側邊欄顯示登出按鈕
        with st.sidebar:
            st.markdown("---")
            logged_user = st.session_state.get("_aqt_user", "")
            st.markdown(f"👤 已登入：**{logged_user}**")
            if st.button("🔓 登出", key="_aqt_logout"):
                st.session_state["_aqt_authenticated"] = False
                st.session_state["_aqt_user"]          = ""
                st.rerun()
        return True

    # ── 顯示登入畫面 ─────────────────────────────────────────
    _render_login_form(title, username, password_hash)
    return False


def _render_login_form(
    title:         str,
    valid_username: str,
    password_hash: str,
) -> None:
    """渲染登入表單，處理暴力破解防護。"""
    import streamlit as st

    # 初始化 session 狀態
    if "_aqt_attempts" not in st.session_state:
        st.session_state["_aqt_attempts"]   = 0
        st.session_state["_aqt_locked_until"] = 0.0

    # 鎖定狀態檢查
    now         = time.time()
    locked_until = st.session_state["_aqt_locked_until"]
    if now < locked_until:
        remain = int(locked_until - now)
        st.error(f"🔒 登入嘗試次數過多，請 {remain} 秒後再試。")
        st.stop()
        return

    # ── 登入表單 ─────────────────────────────────────────────
    st.markdown(f"## 🔐 {title}")
    st.markdown("請輸入帳號密碼以繼續。")

    with st.form("_aqt_login_form", clear_on_submit=True):
        uname_input = st.text_input("帳號", placeholder="username")
        pwd_input   = st.text_input("密碼", type="password", placeholder="password")
        submitted   = st.form_submit_button("登入")

    if submitted:
        # 使用者名稱比對（timing-safe）
        uname_match = hmac.compare_digest(
            uname_input.strip().lower(),
            valid_username.strip().lower(),
        )
        pwd_match = verify_password(pwd_input, password_hash)

        if uname_match and pwd_match:
            st.session_state["_aqt_authenticated"] = True
            st.session_state["_aqt_user"]          = uname_input
            st.session_state["_aqt_attempts"]      = 0
            st.session_state["_aqt_locked_until"]  = 0.0
            log.info(f"[Auth] 登入成功: {uname_input}")
            st.rerun()
        else:
            st.session_state["_aqt_attempts"] += 1
            attempts = st.session_state["_aqt_attempts"]
            if attempts >= _MAX_ATTEMPTS:
                st.session_state["_aqt_locked_until"] = time.time() + _LOCKOUT_SEC
                st.session_state["_aqt_attempts"]     = 0
                log.warning(f"[Auth] 登入失敗 {attempts} 次，鎖定 {_LOCKOUT_SEC}s")
                st.error(f"🔒 連續失敗 {_MAX_ATTEMPTS} 次，帳號鎖定 {_LOCKOUT_SEC//60} 分鐘。")
            else:
                remaining = _MAX_ATTEMPTS - attempts
                st.error(f"帳號或密碼錯誤（剩餘嘗試次數：{remaining}）")
            log.warning(
                f"[Auth] 登入失敗 username={uname_input!r} "
                f"attempts={st.session_state['_aqt_attempts']}"
            )

    st.stop()


# ════════════════════════════════════════════════════════════
# CLI 工具：python -m quant_system.utils.ui_auth hash
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2 and sys.argv[1] == "hash":
        import getpass
        print("=== AQT UI 密碼雜湊生成工具 ===")
        pwd = getpass.getpass("請輸入密碼（不顯示）：")
        if not pwd:
            print("錯誤：密碼不能為空")
            sys.exit(1)
        hashed = hash_password(pwd)
        print(f"\n請將以下內容加入 .env 或 .streamlit/secrets.toml：\n")
        print(f"  [環境變數]  UI_PASSWORD_HASH={hashed}")
        print(f"\n  [secrets.toml 格式]")
        print(f"  [auth]")
        print(f'  username      = "your_username"')
        print(f'  password_hash = "{hashed}"')
    else:
        print("用法：python -m quant_system.utils.ui_auth hash")
        print("      產生可安全儲存的密碼雜湊字串")
