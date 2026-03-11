from pathlib import Path
from sqlalchemy import text
from src.mrs.data.database import get_engine

SQL_DIR = Path("data/sql")


def run_sql_file(file_path: Path):
    sql = file_path.read_text(encoding="utf-8")
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"Applied: {file_path.name}")


def main():
    files = [
        SQL_DIR / "schema.sql",
        SQL_DIR / "indexes.sql",
    ]

    for file_path in files:
        run_sql_file(file_path)


if __name__ == "__main__":
    main()