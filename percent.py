# percent.py
# Given a stream of timestamped boolean values (e.g. "up" measures)
# ...calculate the percentage time when each is true

import pandas as pd
import numpy as np
import time
import datetime
import sys
from line_profiler import LineProfiler

NUM_BINS = 10
BIN_SIZE = pd.Timedelta(minutes=5)   # What size time bins do we want on the output?

names =     ["$ts", "$id", "up", "version"]
rows = [
            [pd.Timestamp(datetime.datetime(2021,1,1,00,00,00)), "A", 1, "1"],    # Keyframe (contains all properties for all devices. May be timestamped in the past)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,00,00)), "B", 1, "1"],
            [pd.Timestamp(datetime.datetime(2021,1,1,00,00,00)), "C", 1, "2"],   

            [pd.Timestamp(datetime.datetime(2021,1,1,00, 2,00)), "A", 1],         # Heartbeats every 5 minutes
            [pd.Timestamp(datetime.datetime(2021,1,1,00, 3,00)), "B", 1, "2"],    # Device B gets upgraded to version 2
            [pd.Timestamp(datetime.datetime(2021,1,1,00, 4,00)), "C", 1],

                                                                                  # A and B both stop talking
            [pd.Timestamp(datetime.datetime(2021,1,1,00, 9,00)), "C", 1],         # C is still talking

            [pd.Timestamp(datetime.datetime(2021,1,1,00,14,00)), "C", 1],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,17,00)), "A", 0],         # A and B marked as offline by timer (code for this is elsewhere :-) 
            [pd.Timestamp(datetime.datetime(2021,1,1,00,18,00)), "B", 0],          
            [pd.Timestamp(datetime.datetime(2021,1,1,00,19,00)), "C", 1],          

            [pd.Timestamp(datetime.datetime(2021,1,1,00,24,00)), "C", 1],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,27,00)), "A", 1],         # A comes back online, after 10 minutes offline (it's been running version 1 this whole time)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,29,00)), "C", 1],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,32,00)), "A", 1],
            [pd.Timestamp(datetime.datetime(2021,1,1,00,34,00)), "C", 1],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,37,00)), "A", 1],          
            [pd.Timestamp(datetime.datetime(2021,1,1,00,38,00)), "B", 1],         # B comes back online, after 20 minutes offline (it was running version 1, but changed to version 2 before it went offline)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,39,00)), "C", 1],

    ]

def percent_of_time_where(df):

    # Create bins
    all_ids = df["$id"].unique()
    num_ids = len(all_ids)
    
    bins = pd.DataFrame()
    date = pd.Timestamp("2021-01-01T00:00:00")
    for b in range(NUM_BINS):  #  Relatively few bins, so OK to iterate at high level
        dates = np.full(num_ids, date)  # Twice as fast as using Python lists
        bin_numbers = np.full(num_ids, b)
        new_bins = pd.DataFrame({'$ts' : dates, '$id' : all_ids, 'bin_number' : bin_numbers})
        bins = pd.concat([bins, new_bins])
        date += BIN_SIZE
    
    # these_bins = pd.DataFrame({'$ts' : pd.bdate_range('2021-01-01', freq='5min', periods = NUM_BINS).tolist(), 'bin_number' : range(NUM_BINS)})
    
    bins.set_index('$ts')   # Don't know if we need this, but v. fast.
    
    # Merge bins with existing events (can we use merge(), to avoid sort()? Everything already sorted. TODO: Try sort_index() if we ensure the indices are correct first
    df = pd.concat([df, bins], ignore_index=True) # Some functions hate duplicate indexes, so re-index (most don't pay any attention). Doesn't affect speed of this.
    df = df.sort_values(by="$ts", kind="mergesort") # Mergesort fastest, because largely already sorted
     
    groups = df.groupby("$id")
    
    # df["version"] = groups["version"].fillna(method='ffill')  # 20x faster than ffill()
    # df["up"] = groups["up"].fillna(method='ffill')
    # df["bin_number"] = groups["bin_number"].fillna(method='ffill') 
    df[["version", "up", "bin_number"]] = groups[["version", "up", "bin_number"]].ffill() # Quite slow - 50ms

    df['time_delta'] = groups['$ts'].shift(-1) - df['$ts'] 
    # df['time_delta'] = groups['$ts'].transform(lambda x : x.diff().shift(-1)) # Slow
    # df['time_delta'] = groups['$ts'].diff()    # Slow if lots of groups (https://stackoverflow.com/questions/53150700/why-the-groupby-diff-is-so-slower)
    # df['time_delta'] = groups['time_delta'].shift(-1)     # Forward-looking deltas

    print(df)
    df['up_time'] = df['time_delta'] * df['up']
     
    result= df.groupby(['$id', 'bin_number'])['up_time'].sum()

    return result

def run_percent():
    global df_big
    result = percent_of_time_where(df_big)

df = pd.DataFrame(rows, columns = names)
df.set_index("$ts")

print("Functional test")
result = percent_of_time_where(df)
print(result)

# sys.exit(0)

print("Performance test")
df_big = pd.DataFrame(rows, columns = names)
for i in range(16):
    df2 = df_big.copy()    # Create an identical dataframe later in time
    span = df2["$ts"].iloc[-1] - df2["$ts"].iloc[0]
    df2["$ts"] += span
    df_big = pd.concat([df_big, df2], ignore_index=True)    # Ignore index so we get a nice monotonic, non-duplicate index (which will make some functions much faster)
# Randomise IDs to 100k values
df_big['$id'] = np.random.randint(1, 100_000, df_big.shape[0])  # This slows it down from around 1.2M rows/sec to around 14k!
print(df_big)

lprofiler = LineProfiler()
lprofiler.add_function(percent_of_time_where)
lp_wrapper = lprofiler(run_percent)
t1 = time.time()
lp_wrapper()
t2 = time.time()
lprofiler.print_stats()

print("Took {time:,}ms to do {rows:,} rows, which is {speed:,} rows/sec".format(
    time=int((t2-t1)*1000),
    rows=len(df_big),
    speed=int(len(df_big)/(t2-t1)))
    )

