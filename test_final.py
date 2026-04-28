#!/usr/bin/env python3
"""
最終測試腳本：驗證完整的認證系統
"""
import requests
import json
import os
import time

def test_system():
    """測試完整系統"""
    base_url = "http://localhost:8000"
    
    print("AQT 交易系統最終測試")
    print("=" * 50)
    
    # 1. 測試根路徑重定向
    print("1. 測試根路徑重定向...")
    try:
        response = requests.get(f"{base_url}/")
        if response.status_code == 200:
            print("✅ 根路徑正常重定向到登入頁面")
        else:
            print(f"❌ 根路徑訪問失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ 根路徑訪問錯誤: {e}")
    
    # 2. 測試登入頁面
    print("2. 測試登入頁面...")
    try:
        response = requests.get(f"{base_url}/login.html")
        if response.status_code == 200:
            print("✅ 登入頁面可正常訪問")
        else:
            print(f"❌ 登入頁面訪問失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ 登入頁面訪問錯誤: {e}")
    
    # 3. 測試Dashboard頁面
    print("3. 測試Dashboard頁面...")
    try:
        response = requests.get(f"{base_url}/dashboard.html")
        if response.status_code == 200:
            print("✅ Dashboard頁面可正常訪問")
        else:
            print(f"❌ Dashboard頁面訪問失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ Dashboard頁面訪問錯誤: {e}")
    
    # 4. 測試認證API端點
    print("4. 測試認證API端點...")
    
    # 測試未認證訪問
    try:
        response = requests.get(f"{base_url}/api/balance")
        if response.status_code == 401:
            print("✅ 未認證訪問被正確阻止")
        else:
            print(f"❌ 未認證訪問狀態: {response.status_code}")
    except Exception as e:
        print(f"❌ 未認證訪問測試錯誤: {e}")
    
    # 測試登入
    password = os.getenv("UI_PASSWORD", "aqt123")
    login_data = {"password": password}
    try:
        response = requests.post(f"{base_url}/api/login", json=login_data)
        if response.status_code == 200:
            result = response.json()
            token = result.get("token")
            print(f"✅ 登入成功，獲得token: {token[:10]}...")
            
            # 測試認證訪問
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{base_url}/api/balance", headers=headers)
            if response.status_code == 200:
                print("✅ 認證訪問成功")
            else:
                print(f"❌ 認證訪問失敗: {response.status_code}")
            
            # 測試交易模式切換
            mode_data = {"mode": "SEMI_AUTO"}
            response = requests.post(
                f"{base_url}/api/execution_mode",
                json=mode_data,
                headers=headers
            )
            if response.status_code == 200:
                result = response.json()
                print(f"✅ 模式切換成功: {result.get('message')}")
            else:
                print(f"❌ 模式切換失敗: {response.status_code}")
            
            # 測試登出
            response = requests.post(f"{base_url}/api/logout", headers=headers)
            if response.status_code == 200:
                print("✅ 登出成功")
            else:
                print(f"❌ 登出失敗: {response.status_code}")
                
        else:
            print(f"❌ 登入失敗: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ 登入測試錯誤: {e}")
    
    print("\n測試完成！")
    print("\n使用說明：")
    print("1. 訪問 http://localhost:8000 自動跳轉到登入頁面")
    print("2. 使用 .env 中設定的 UI_PASSWORD 登入")
    print("3. 在Dashboard頂部可切換 FULL_AUTO/SEMI_AUTO 模式")
    print("4. 所有API端點都需要認證才能訪問")

if __name__ == "__main__":
    test_system()