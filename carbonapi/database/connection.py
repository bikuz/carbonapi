from django.conf import settings
import psycopg2

def get_foris_connection():
    db = settings.DATABASES['nfi']
    conn_str = (
        f"host={db['HOST']} "
        f"dbname={db['NAME']} "
        f"user={db['USER']} "
        f"password={db['PASSWORD']} "
        f"port={db.get('PORT', 5432)}"
    )
    # print('connection string',conn_str)
    return psycopg2.connect(conn_str)