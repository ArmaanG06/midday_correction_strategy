import duckdb
from pathlib import Path

#def get existing data from databse(ticker, start_date, end_date, bar_interval):

def pull_existing_data(ticker, start_date, end_date, bar_interval, base_path, dataset:str="us_equities"):

    




    conn = duckdb.connect(database=":memory:")
    partition_path = (Path(base_path)/f"dataset={dataset}"/f"bar_interval={bar_interval}"/f"ticker={ticker}"/f"year={year}"/f"month={month:02d}"/f"day={day:02d}")

    df = conn.execute(f"""
    SELECT * FROM {partition_path}
    """).fetch_df()

    return df

    out["datetime"] = pd.to_datetime(out["dateime"], utc=True)
    out["bar_interval"] = bar_interval
    year = out["datetime"].dt.year.astype("int32")
    month = out["datetime"].dt.month.astype("int8")
    day = out["datetime"].dt.day.astype("int8")



    df = conn.execute(f"""
    SELECT * 
    FROM {partition_path}
    """).fetch_df()
    return df
    conn.close()
    
