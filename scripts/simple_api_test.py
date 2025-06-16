import requests

BASE_URL = "http://localhost:8000"

def test_api_connection():
    """Simple test to verify API connectivity"""
    url = f"{BASE_URL}/health"
    print("\nTesting API connection...")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print("✅ API connection successful!")
            print(f"Response: {response.json()}")
            return True
        else:
            print("❌ API returned error status code:", response.status_code)
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to API - Is it running?")
        print("Hint: Start the API with 'uvicorn src.api.main:app --reload'")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    test_api_connection()