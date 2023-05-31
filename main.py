from utils import download_data, create_table, create_view, two_b_1, two_b_2
import duckdb

PERSISTENT_STORAGE_PATH = "./data/data.db"


def main():
    conn = duckdb.connect(database=PERSISTENT_STORAGE_PATH, read_only=False)
    download_data()
    create_table(conn)
    create_view(conn)
    two_b_1(conn)
    two_b_2(conn)


if __name__ == "__main__":
    main()
