import pandas as pd

def create_date_table(
        start='2000-01-01',
          end='2050-12-31'
    ):
    """
    Creates a date dimension table that should not need to be modified
    after creation

    Should only be ran once to generate original table
    """
   
    df = pd.DataFrame(
        {"Date": pd.date_range(start, end)}
        )
    df.index.name = "date_id"
    df["year"] = df.Date.dt.year
    df["month"] = df.Date.dt.month
    df["day"] = df.Date.dt.day
    df["day_of_week"] = df.Date.dt.day_of_week
    df["day_name"] = df.Date.dt.day_name()
    df["month_name"] = df.Date.dt.month_name()
    df["quarter"] = df.Date.dt.quarter
    df = df.drop("Date", axis=1)
    return df
