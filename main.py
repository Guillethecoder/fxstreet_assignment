from utils import download_data, create_table, create_view, two_b_1, two_b_2, two_b_3
import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

PERSISTENT_STORAGE_PATH = "/data/data.db"

def main():
    conn = download_data()
    conn = duckdb.connect(database=PERSISTENT_STORAGE_PATH, read_only=False)
    create_table(conn)
    create_view(conn)
    two_b_1(conn)
    two_b_2(conn)
    df = two_b_3(conn)
    conn.close()
    return df.to_json(orient='records')


app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/v1/main")
async def root():
    return main()