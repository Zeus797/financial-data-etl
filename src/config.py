from sqlalchemy.engine.url import URL

DATABASE = {
    'drivername': 'postgresql',
    'host': 'localhost',
    'port': 5432,
    'username': 'postgres',
    'password': 'your_password',
    'database': 'your_database'
}

DATABASE_URL = URL.create(**DATABASE)