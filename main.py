from utils import *
import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PERSISTENT_STORAGE_PATH = "/data/data.db"

def main():
    conn = download_data()
    conn = duckdb.connect(database=PERSISTENT_STORAGE_PATH, read_only=False)
    create_table(conn)
    create_view(conn)
    create_my_table(conn)
    calculate_purchases_and_revenue_per_product_week(conn)
    calculate_number_of_users_per_step_per_week(conn)
    df = calculate_conversion_rate_per_step_per_week(conn)
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