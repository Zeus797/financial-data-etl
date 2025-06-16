# scripts/test_api.py
"""
Simple test script to verify API functionality
"""
import requests
import json
from datetime import date, timedelta
from typing import Dict, Any

BASE_URL = "http://localhost:8000"

def test_endpoint(name: str, url: str, method: str = "GET", 
                 data: Dict[str, Any] = None, params: Dict[str, Any] = None):
    """Test an API endpoint and print results"""
    print(f"\n{'='*50}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, params=params) if method == "GET" else \
                  requests.post(url, json=data, params=params)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Success!")
            data = response.json()
            if isinstance(data, list):
                print(f"Response contains {len(data)} items")
                if data: print("First item:", json.dumps(data[0], indent=2, default=str))
            elif isinstance(data, dict):
                print("Response:", json.dumps(data, indent=2, default=str)[:500])
        else:
            print("❌ Failed!")
            print("Error:", response.text)
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error - Is the API running?")
    except Exception as e:
        print(f"❌ Error: {str(e)}")

def run_essential_tests():
    """Run core API tests"""
    print("Starting Essential API Tests...")
    print(f"Base URL: {BASE_URL}\n")
    
    # Core functionality tests
    tests = [
        ("Health Check", "/health"),
        ("Get Currency Pairs", "/api/v1/forex/pairs"),
        ("Latest USD/KES Rate", "/api/v1/forex/rates/USD/KES/latest"),
        ("Historical USD/KES Data", "/api/v1/forex/rates/USD/KES/history", {
            "start_date": (date.today() - timedelta(days=30)).isoformat(),
            "end_date": date.today().isoformat(),
            "limit": 5
        })
    ]
    
    for test in tests:
        name, endpoint = test[0], test[1]
        params = test[2] if len(test) > 2 else None
        test_endpoint(name, f"{BASE_URL}{endpoint}", params=params)

def test_african_pairs():
    """Test essential African currency pairs"""
    print("\n=== Testing African Currency Pairs ===")
    
    # Test only the most important pairs
    pairs = [("USD", "KES"), ("USD", "ZAR"), ("USD", "NGN")]
    
    for base, quote in pairs:
        pair_name = f"{base}/{quote}"
        print(f"\nTesting {pair_name}")
        test_endpoint(
            f"Latest {pair_name} Rate",
            f"{BASE_URL}/api/v1/forex/rates/{base}/{quote}/latest"
        )

if __name__ == "__main__":
    run_essential_tests()
    test_african_pairs()