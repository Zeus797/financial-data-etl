"""
Script to run the Financial Data Pipeline API
"""
import uvicorn
import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

if __name__ == "__main__":
    # Run the API server
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Enable auto-reload during development
        log_level="info",
        access_log=True
    )

# Alternative configurations for different environments:

# Production configuration
def run_production():
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        workers=4,  # Multiple workers for production
        log_level="info",
        access_log=True,
        reload=False
    )

# Development configuration with custom settings
def run_development():
    uvicorn.run(
        "src.api.main:app",
        host="127.0.0.1",
        port=8001,
        reload=True,
        reload_dirs=["src"],
        log_level="debug",
        access_log=True
    )
