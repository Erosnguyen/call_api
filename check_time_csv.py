import pandas as pd
from datetime import datetime, time

def check_time_range(df, time_column, start_time, end_time,start_time2, end_time2):
    df[time_column] = pd.to_datetime(df[time_column])
    in_range = df[((df[time_column].dt.time.between(start_time, end_time)) | (df[time_column].dt.time.between(start_time2, end_time2)))]
    return in_range

def time_range(df):
    start_time = time(8, 45, 0)
    end_time = time(11, 30, 0)
    start_time2 = time(13, 0, 0)
    end_time2 = time(15, 0, 0)
    result = check_time_range(df, 'Time', start_time, end_time,start_time2, end_time2) 
    return result





