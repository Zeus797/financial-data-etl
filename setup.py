from setuptools import setup, find_packages

setup(
    name="financial_data_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'sqlalchemy',
        'psycopg2-binary',
        'aiohttp',
        'asyncio',
        'python-dotenv'
    ]
)