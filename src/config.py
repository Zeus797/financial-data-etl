from sqlalchemy.engine.url import URL

DATABASE = {
    'drivername': 'postgresql',
    'host': 'localhost',
    'port': 5432,
    'username': 'postgres',
    'password': 'postgres123',
    'database': 'postgres'
}

DATABASE_URL = URL.create(**DATABASE)


