from sqlalchemy import text
from src.mrs.data.db import get_engine


def main():
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("select 1"))
        print(result.scalar())


if __name__ == "__main__":
    main()