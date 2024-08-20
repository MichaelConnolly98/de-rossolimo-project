"placeholder so not empty"
import pandas as pd

def load_processed(df):
    pq = pd.DataFrame.to_parquet(df)
    return pq