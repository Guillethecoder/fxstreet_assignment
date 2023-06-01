import requests
import duckdb
import pandas as pd
import os

def download_data(url: str = "https://sde-test-data-sltezl542q-ew.a.run.app/", path: str = "/data/file.parquet") -> None:
    """
    This function downloads the data from the url and saves it to data/file.parquet

    Args:
        url (str, optional): URL to the data. Defaults to "https://sde-test-data-sltezl542q-ew.a.run.app/".
        path (str, optional): Path to save the data. Defaults to "/data/file.parquet".
    """
    response = requests.get(url, stream=True)
    with open(path, "wb") as f:
        f.write(response.content)
    print("Data downloaded successfully")


def create_table(
    conn: duckdb.connect, table_name: str = "events",
        path: str = "/data/file.parquet") -> None:
    """
    This function creates the table from the parquet file.
    The table is called events by default and has the following columns:
    event_timestamp, event_name, event_params, user_id, user_pseudo_id, session_id
    
    Args:
        conn (duckdb.connect): Connection to the database
        table_name (str, optional): Name of the table. Defaults to "events".
        path (str, optional): Path to the parquet file. Defaults to "data/file.parquet".
    """
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            "event_timestamp"::BIGINT AS event_timestamp,
            "event_name"::VARCHAR AS event_name,
            "event_params"::
                STRUCT(
                    key VARCHAR, value STRUCT(
                        int_value INTEGER, string_value VARCHAR)
                    )[] AS event_params,
            "user_id"::VARCHAR AS user_id,
            "user_pseudo_id"::VARCHAR AS user_pseudo_id,
            "session_id"::BIGINT AS session_id,
        FROM read_parquet('{path}')
    """)
    print(f"Table {table_name} created successfully")

def test_download_data():
    """
    This function tests if the data has been downloaded successfully.
    """
    download_data(path = "./data/file.parquet")
    assert os.path.exists("./data/file.parquet")

def test_create_table():
    """
    This function tests if the table events has been created successfully.
    """
    conn = duckdb.connect(database='./data/data.db', read_only=False)
    create_table(conn, path = "./data/file.parquet")
    assert conn.execute("""SELECT * FROM events LIMIT 1""").fetch_df().shape[0] == 1


def create_view(conn: duckdb.connect, view_name: str = 'events_unnested') -> None:
    """
    This function creates a view from the table events.
    The view is called events_unnested by default and has the following columns:
    event_timestamp, event_name, key, int_value, string_value, user_id, user_pseudo_id, session_id

    Args:
        conn (duckdb.connect): Connection to the database
        view_name (str, optional): Name of the view. Defaults to "events_unnested".
    """
    conn.execute(f"""
    CREATE OR REPLACE VIEW {view_name} AS
        SELECT event_timestamp, event_name,
        UNNEST(event_params).key as key,
        UNNEST(event_params).value.int_value as int_value,
        UNNEST(event_params).value.string_value as string_value,
        user_id, user_pseudo_id, session_id 
    from events
    """)
    print(f"View {view_name} created successfully")


def legacy_two_b_1(conn: duckdb.connect) -> pd.DataFrame:
    """
    This function performs calculation directly from the events_unnested view.
    This function calculates the revenue for each product and week.
    The revenue is calculated as the sum of the amount for each product and week.

    Args:
        conn (duckdb.connect): Connection to the database
    
    Returns:
        pd.DataFrame: Dataframe with the revenue for each product and week
    """
    return conn.execute("""
    SELECT t2.product ,SUM(t1.int_value) as revenue, COUNT(t2.session_id)
        as purchases, week(epoch_ms(t1.event_timestamp)) as week from events_unnested as t1 
    join(
        SELECT session_id, string_value as product
            FROM events_unnested 
            WHERE key = 'product')
        as t2
    ON t1.session_id = t2.session_id
    WHERE t1.key = 'amount'
    GROUP BY t2.product, week
    """).fetch_df()

def legacy_two_b_2(conn: duckdb.connect):
    print(conn.execute("""
    This function performs calculation directly from the events_unnested view.
    SELECT string_value, count(distinct user_pseudo_id) as users,
        week(epoch_ms(event_timestamp)) as week 
    from events_unnested
    WHERE key = 'step'
    GROUP BY string_value, week
    """).fetch_df())


def legacy_two_b_3_select_one(conn: duckdb.connect, step: str = 'landing') -> pd.DataFrame:
    """
    This function performs calculation directly from the events_unnested view.
    This function calculates the conversion rate for a given step in the funnel.
    The conversion rate is calculated as the number of users who completed the step divided by the number of users who started the step.
    
    Args:
        conn (duckdb.connect): Connection to the database
        step (str, optional): Step in the funnel. Defaults to 'landing'.
    
    Returns:
        pd.DataFrame: Dataframe with the conversion rate for the given step
    """

    return conn.execute(f"""
        with t1 as (
            SELECT DISTINCT ON(session_id) session_id, week(epoch_ms(event_timestamp)) as week, list(string_value) over w1 as steps,        
                from events_unnested
                WHERE key = 'step'
                window w1 as (
                PARTITION BY session_id, week(epoch_ms(event_timestamp)))
        )
        SELECT
            DISTINCT ON (week) week,
            count(*) FILTER (WHERE list_contains(steps, '{step}')) over (PARTITION BY week) as total,
            count(*) FILTER (WHERE steps[-1] = '{step}') over (PARTITION BY week) as dropped,
            (total::DOUBLE - dropped::DOUBLE) / total::DOUBLE * 100 as conversion_rate,
            '{step}' as step
        FROM t1;
    """).fetch_df()
    
def legacy_two_b_3(conn: duckdb.connect) -> pd.DataFrame:
    """
    This function performs calculation directly from the events_unnested view.
    This function calculates the conversion rate for each step in the funnel.
    The conversion rate is calculated as the number of users who completed the step divided by the number of users who started the step.
    Args:
        conn (duckdb.connect): Connection to the database

    Returns:
        pd.DataFrame: Dataframe with the conversion rate for each step in the funnel
    """
    return pd.concat(
    conn.execute(f"""
        with t1 as (
            SELECT DISTINCT ON(session_id) session_id, week(epoch_ms(event_timestamp)) as week, list(string_value) over w1 as steps,        
                from events_unnested
                WHERE key = 'step'
                window w1 as (
                PARTITION BY session_id, week(epoch_ms(event_timestamp)))
        )
        SELECT
            DISTINCT ON (week)
            count(*) FILTER (WHERE list_contains(steps, '{i}')) over (PARTITION BY week)  as total,
            count(*) FILTER (WHERE steps[-1] = '{i}') over (PARTITION BY week) as dropped,
            round((total::DOUBLE - dropped::DOUBLE) / total::DOUBLE * 100, 2) as conversion_rate,
            '{i}' as step,
            week
        FROM t1
        QUALIFY 
            total > 0;
        """).fetch_df() for i in conn.execute("""
        select distinct string_value from events_unnested where key = 'step' """).fetch_df()['string_value']
    )



def create_my_table(conn: duckdb.connect) -> None:
    """
    This function creates the table that I propose to use to answer the queries in the
    assignment. The table is called my_table and has the following columns:
    session_id, user_pseudo_id, week, year, steps, product, amount, currency
    To find a more detailed description of the table, please refer to the notebook.py file.
    """
    conn.execute("""
    CREATE OR REPLACE TABLE my_table AS (
        SELECT DISTINCT ON(session_id) session_id, user_pseudo_id, week(epoch_ms(event_timestamp)) as week,
        year(epoch_ms(event_timestamp)) as year,
        list(string_value) FILTER (WHERE key = 'step') over w1 as steps,
        string_agg(string_value, '') FILTER (WHERE key = 'product') over w1 as product,
        sum(int_value) FILTER (WHERE key = 'amount') over w1 as amount,
        string_agg(string_value, '') FILTER (WHERE key = 'currency') over w1 as currency,
            from events_unnested
            window w1 as (
            PARTITION BY session_id, year(epoch_ms(event_timestamp)), week(epoch_ms(event_timestamp))
        )
    )"""
    )
    print("Table my_table created successfully")


def calculate_purchases_and_revenue_per_product_week(conn: duckdb.connect) -> pd.DataFrame():
    """
    This function calculates the purchases and revenue for each week.
    The purchases are calculated as the number of sessions that have a purchase.
    The revenue is calculated as the sum of the amount for each session.

    Args:
        conn (duckdb.connect): Connection to the database
    
    Returns:
        pd.DataFrame: Dataframe with the purchases and revenue for each week
    """
    return conn.execute("""
    SELECT
        product,
        week,
        count(*) as purchases,
        sum(amount) as revenue
    FROM my_table
    WHERE product IS NOT NULL
    GROUP BY product, week
    ORDER BY week, product
    """).fetch_df()

def calculate_number_of_users_per_step_per_week(conn: duckdb.connect) -> pd.DataFrame:
    """
    This function calculates the number of users for each step and week.
    The number of users is calculated as the number of distinct user_pseudo_id for each step and week.
    
    Args:
        conn (duckdb.connect): Connection to the database
    
    Returns:
        pd.DataFrame: Dataframe with the number of users for each step and week
    """
    return conn.execute("""
    SELECT
        DISTINCT ON (week)
        count(*) FILTER (WHERE list_contains(steps, 'landing')) over (PARTITION BY year, week)  as landing,
        count(*) FILTER (WHERE list_contains(steps, 'checkout')) over (PARTITION BY year, week)  as checkout,
        count(*) FILTER (WHERE list_contains(steps, 'login-options')) over (PARTITION BY year, week)  as login_options,
        count(*) FILTER (WHERE list_contains(steps, 'sign-up')) over (PARTITION BY year, week)  as sign_up,
        count(*) FILTER (WHERE list_contains(steps, 'purchase')) over (PARTITION BY year, week)  as purchase,
        year, week
        FROM my_table
    """).fetch_df()

def calculate_conversion_rate_per_week(conn: duckdb.connect, step:str) -> pd.DataFrame:
    """
    This function calculates the conversion rate per week for a given step.
    The conversion rate is calculated as the number of users who arrived the step divided minus number of users who dropped
    divided by the number of users who arrived the step.

    Args:
        conn (duckdb.connect): Connection to the database
        step (str): Step in the funnel
    
    Returns:
        pd.DataFrame: Dataframe with the conversion rate for the given step
    """
    return conn.execute(f"""
        SELECT
            DISTINCT ON (week)
            count(*) FILTER (WHERE list_contains(steps, '{step}')) over (PARTITION BY year, week)  as total,
            count(*) FILTER (WHERE steps[-1] = '{step}') over (PARTITION BY year, week) as dropped,
            round((total::DOUBLE - dropped::DOUBLE) / total::DOUBLE * 100, 2) as conversion_rate,
            '{step}' as step,
            week,
            year
        FROM my_table
        QUALIFY 
            total > 0;
        """).fetch_df()

def calculate_conversion_rate_per_step_per_week(conn: duckdb.connect) -> pd.DataFrame:
    """
    This function calculates the conversion rate for each step and week.
    The conversion rate is calculated as the number of users who arrived the step divided minus number of users who dropped
    divided by the number of users who arrived the step.
    
    Args:
        conn (duckdb.connect): Connection to the database
    
    Returns:
        pd.DataFrame: Dataframe with the conversion rate for each step and week
    """
    return pd.concat(
    conn.execute(f"""
        SELECT
            DISTINCT ON (week)
            count(*) FILTER (WHERE list_contains(steps, '{i}')) over (PARTITION BY week)  as total,
            count(*) FILTER (WHERE steps[-1] = '{i}') over (PARTITION BY week) as dropped,
            round((total::DOUBLE - dropped::DOUBLE) / total::DOUBLE * 100, 2) as conversion_rate,
            '{i}' as step,
            week,
            year
        FROM my_table
        QUALIFY 
            total > 0;
        """).fetch_df() for i in conn.execute("""
        select distinct string_value from events_unnested where key = 'step' """).fetch_df()['string_value']
    )
