#!/usr/bin/env python3
"""
測試腳本：驗證新的認證系統和交易模式切換功能
"""

import requests
import json
import os
from pathlib import Path

def test_auth_system():
    """測試認證系統"""
    base_url = "http://localhost:8000"
    
    print("🔐 測試認證系統...")
    
    # 1. 測試未認證訪問
    print("1. 測試未認證訪問API...")
    try:
        response = requests.get(f"{base_url}/api/balance")
        if response.status_code == 401:
            print("   ✅ 未認證訪問被正確阻止")
        else:
            print(f"   ❌ 預期401，實際{response.status_code}")
    except Exception as e:
        print(f"   ❌ 連接錯誤: {e}")
    
    # 2. 測試登入
    print("2. 測試密碼登入...")
    password = os.getenv("UI_PASSWORD", "aqt123")
    login_data = {"password": password}
    
    try:
        response = requests.post(f"{base_url}/api/login", json=login_data)
        if response.status_code == 200:
            result = response.json()
            token = result.get("token")
            print(f"   ✅ 登入成功，獲得token: {token[:10]}...")
            
            # 3. 測試認證訪問
            print("3. 測試認證訪問API...")
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{base_url}/api/balance", headers=headers)
            if response.status_code == 200:
                print("   ✅ 認證訪問成功")
            else:
                print(f"   ❌ 認證訪問失敗: {response.status_code}")
            
            # 4. 測試交易模式切換
            print("4. 測試交易模式切換...")
            mode_data = {"mode": "SEMI_AUTO"}
            response = requests.post(
                f"{base_url}/api/execution_mode", 
                json=mode_data, 
                headers=headers
            )
            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ 模式切換成功: {result.get('message')}")
                
                # 5. 測試模式查詢
                response = requests.get(f"{base_url}/api/execution_mode", headers=headers)
                if response.status_code == 200:
                    result = response.json()
                    print(f"   ✅ 當前模式: {result.get('mode')}")
            else:
                print(f"   ❌ 模式切換失敗: {response.status_code}")
                
            # 6. 測試登出
            print("5. 測試登出...")
            response = requests.post(f"{base_url}/api/logout", headers=headers)
            if response.status_code == 200:
                print("   ✅ 登出成功")
            else:
                print(f"   ❌ 登出失敗: {response.status_code}")
                
            return token
        else:
            print(f"   ❌ 登入失敗: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"   ❌ 登入錯誤: {e}")
    
    return None

def test_pages():
    """測試頁面訪問"""
    base_url = "http://localhost:8000"
    
    print("\n🌐 測試頁面訪問...")
    
    pages = [
        "/login.html",
        "/dashboard.html"
    ]
    
    for page in pages:
        try:
            response = requests.get(f"{base_url}{page}")
            if response.status_code == 200:
                print(f"   ✅ {page} 可正常訪問")
            else:
                print(f"   ❌ {page} 訪問失敗: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {page} 訪問錯誤: {e}")

def main():
    """主測試函數"""
    print("🚀 AQT 交易系統認證功能測試")
    print("=" * 50)
    
    # 檢查服務是否運行
    try:
        response = requests.get("http://localhost:8000/api/ping", timeout=5)
        if response.status_code == 200:
            print("✅ 服務正在運行")
            
            # 運行測試
            test_auth_system()
            test_pages()
            
            print("\n📋 測試完成！")
            print("\n📝 使用說明：")
            print("1. 訪問 http://localhost:8000 自動跳轉到登入頁面")
            print("2. 使用 .env 中設定的 UI_PASSWORD 登入")
            print("3. 在Dashboard頂部可切換 FULL_AUTO/SEMI_AUTO 模式")
            print("4. 所有API端點都需要認證才能訪問")
            
        else:
            print("❌ 服務未正常運行")
    except Exception as e:
        print(f"❌ 無法連接到服務: {e}")
        print("請確保 web_ui.py 正在運行")

if __name__ == "__main__":
    main()