"""
auth_api.py - 密碼登入認證系統
提供簡單的密碼驗證API，用於保護交易系統訪問
"""

import os
import hashlib
import secrets
import time
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# 簡單的JWT風格token管理
class AuthManager:
    def __init__(self):
        self.tokens = {}
        self.password_hash = None
        self.load_password()
    
    def load_password(self):
        """從環境變數載入密碼"""
        password = os.getenv("UI_PASSWORD", "")
        if password:
            # 簡單的SHA256雜湊
            self.password_hash = hashlib.sha256(password.encode()).hexdigest()
        else:
            # 如果沒設定密碼，使用預設密碼（開發模式）
            self.password_hash = hashlib.sha256("aqt123".encode()).hexdigest()
    
    def verify_password(self, password: str) -> bool:
        """驗證密碼"""
        if not password:
            return False
        return hashlib.sha256(password.encode()).hexdigest() == self.password_hash
    
    def create_token(self) -> str:
        """創建新的訪問token"""
        token = secrets.token_urlsafe(32)
        self.tokens[token] = {
            "created_at": datetime.now(),
            "expires_at": datetime.now() + timedelta(hours=24)
        }
        return token
    
    def verify_token(self, token: str) -> bool:
        """驗證token是否有效"""
        if token not in self.tokens:
            return False
        
        token_data = self.tokens[token]
        if datetime.now() > token_data["expires_at"]:
            # Token過期，清理掉
            del self.tokens[token]
            return False
        
        return True
    
    def cleanup_expired_tokens(self):
        """清理過期的token"""
        now = datetime.now()
        expired = [token for token, data in self.tokens.items() 
                  if now > data["expires_at"]]
        for token in expired:
            del self.tokens[token]

# 全局認證管理器
auth_manager = AuthManager()

# 創建獨立的router
auth_router = APIRouter(prefix="/api", tags=["auth"])

# 定義請求模型
class LoginRequest(BaseModel):
    password: str

class ExecutionModeRequest(BaseModel):
    mode: str  # "FULL_AUTO" 或 "SEMI_AUTO"

# HTTP Bearer認證
security = HTTPBearer(auto_error=False)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """獲取當前認證用戶"""
    if not credentials:
        raise HTTPException(status_code=401, detail="未提供認證token")
    
    token = credentials.credentials
    if not auth_manager.verify_token(token):
        raise HTTPException(status_code=401, detail="無效的認證token")
    
    return token

@auth_router.post("/login")
async def login(request: LoginRequest):
    """密碼登入端點"""
    if not auth_manager.verify_password(request.password):
        raise HTTPException(status_code=401, detail="密碼錯誤")
    
    # 清理過期token
    auth_manager.cleanup_expired_tokens()
    
    # 創建新token
    token = auth_manager.create_token()
    
    return {
        "success": True,
        "token": token,
        "message": "登入成功",
        "expires_in": 86400  # 24小時
    }

@auth_router.get("/execution_mode")
async def get_execution_mode(_: str = Depends(get_current_user)):
    """獲取當前執行模式"""
    current_mode = os.getenv("TRADE_EXECUTION_MODE", "FULL_AUTO").upper()
    return {
        "mode": current_mode,
        "available_modes": ["FULL_AUTO", "SEMI_AUTO"]
    }

@auth_router.post("/execution_mode")
async def set_execution_mode(
    request: ExecutionModeRequest, 
    _: str = Depends(get_current_user)
):
    """設置執行模式"""
    mode = request.mode.upper()
    if mode not in ["FULL_AUTO", "SEMI_AUTO"]:
        raise HTTPException(status_code=400, detail="無效的執行模式")
    
    # 更新環境變數
    os.environ["TRADE_EXECUTION_MODE"] = mode
    
    # 嘗試更新.env文件
    try:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            lines = env_path.read_text(encoding="utf-8").splitlines()
            found = False
            for i, line in enumerate(lines):
                if line.startswith("TRADE_EXECUTION_MODE"):
                    lines[i] = f"TRADE_EXECUTION_MODE={mode}"
                    found = True
                    break
            if not found:
                lines.append(f"TRADE_EXECUTION_MODE={mode}")
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception as e:
        # 如果無法寫入.env，只在內存中更新
        pass
    
    return {
        "success": True,
        "mode": mode,
        "message": f"已切換至{mode}模式"
    }

@auth_router.get("/verify")
async def verify_token(_: str = Depends(get_current_user)):
    """驗證token有效性"""
    return {"valid": True, "message": "token有效"}

@auth_router.post("/logout")
async def logout(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """登出端點"""
    if credentials and credentials.credentials in auth_manager.tokens:
        del auth_manager.tokens[credentials.credentials]
    return {"success": True, "message": "已登出"}