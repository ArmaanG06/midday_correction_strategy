import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

_engine = None


def get_engine():
    global _engine

    if _engine is None:
        db_url = os.getenv("SUPABASE_DB_URL")

        if not db_url:
            raise RuntimeError("SUPABASE_DB_URL not found in .env")

        _engine = create_engine(
            db_url,
            pool_pre_ping=True,
        )

    return _engine