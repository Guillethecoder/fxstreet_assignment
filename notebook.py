# %%
import duckdb
import pandas as pd
import requests
conn = duckdb.connect(database='./data/data.db', read_only=False)


#STEP 1:
# %%
url = 'https://sde-test-data-sltezl542q-ew.a.run.app/'
response = requests.get(url, stream=True)
with open("data/file.parquet", "wb") as f:
    f.write(response.content)

# With this, we have now downloaded the data and saved it to data/file.parquet
# Now we can create the table from the parquet file.

# %%
conn.execute("""
    CREATE OR REPLACE TABLE 'events' AS
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
    FROM read_parquet('data/file.parquet')
""")

conn.execute("""
    SELECT * FROM events LIMIT 10
""").fetch_df()

# As we can see, the table has been created successfully.


# STEP 2.A:

# %%
conn.execute(f"""
    CREATE OR REPLACE VIEW events_unnested AS
        SELECT event_timestamp, event_name,
        UNNEST(event_params).key as key,
        UNNEST(event_params).value.int_value as int_value,
        UNNEST(event_params).value.string_value as string_value,
        user_id, user_pseudo_id, session_id 
    from events
    """)
conn.execute("""
    SELECT * FROM events_unnested LIMIT 10
""").fetch_df()

# We have now created the view events_unnested from the table events.
# The view has the following columns:
# event_timestamp, event_name, key, int_value, string_value, user_id, user_pseudo_id, session_id

# STEP 2.B:

# We can create the table I want to use for the analysis by using the following query:

# %%
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

# The table has the following columns:
# session_id, user_pseudo_id, week, year, steps, product, amount, currency
# Where steps is an ordered list of the steps the user has taken in the funnel.
# The product is the product the user has bought. NaN if no purchase was made.
# The amount is the amount the user has spent. Nan if no purchase was made.
# The currency is the currency the user has spent in. Nan if no purchase was made.

# This table can be used to answer the questions in the assignment.
#  PROS: 
#  - The table is easy to understand and can be used to answer the questions in the assignment.
#  - The table is easy to query.
#  - It is easyer to query this table compared to the view.
#  CONS:
#  - The table is not normalized.
#  - The table is not easy to update if the data changes.

# STEP 2.C:

# Now I will perform the queries to answer the questions in the assignment.

# 1. Purchases and revenue per product and week (assume all currencies are USD)

# %%
conn.execute("""
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

# Number of users per week at each step of the funnel

# %%
conn.execute("""
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

# 3. Conversion rate per step of the funnel per week
# %%
#The following version is in order to get the conversion rate for one step of the funnel. 
#In this example we're doing it for the step 'landing'.
conn.execute("""
    SELECT
        DISTINCT ON (week)
        count(*) FILTER (WHERE list_contains(steps, 'landing')) over (PARTITION BY year, week)  as total,
        count(*) FILTER (WHERE steps[-1] = 'landing') over (PARTITION BY year, week) as dropped,
        (total::DOUBLE - dropped::DOUBLE) / total::DOUBLE * 100 as conversion_rate,
        'landing' as step,
        year,
        week
    FROM my_table
    QUALIFY
        total > 0;
    ;
""").fetch_df()

# %%
#The following version is in order to get the conversion rate for all steps of the funnel.
pd.concat(
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


# Note: I've also done the queries to get the answers with the view events_unnested. They are in the file utils.py
# The ones with the legacy prefix are the ones that use the view.