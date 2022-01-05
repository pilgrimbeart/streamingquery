import pandas as pd
import numpy as np
import datetime
import time
import sys
from line_profiler import LineProfiler


TIMEOUT = pd.Timedelta(minutes=15)  # How long does a device have to be silent before we deem it to be offline?
BINSIZE = pd.Timedelta(minutes=5)   # What size time bins do we want on the output?

names =     ["$ts", "$id", "version"]
rows = [
            [pd.Timestamp(datetime.datetime(2021,1,1,00,00,00)), "A", "1"],    # Keyframe (contains all properties for all devices. May be timestamped in the past)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,00,00)), "B", "1"],
            [pd.Timestamp(datetime.datetime(2021,1,1,00,00,00)), "C", "2"],   

            [pd.Timestamp(datetime.datetime(2021,1,1,00, 2,00)), "A"],         # Heartbeats every 5 minutes
            [pd.Timestamp(datetime.datetime(2021,1,1,00, 3,00)), "B", "2"],    # Device B gets upgraded to version 2
            [pd.Timestamp(datetime.datetime(2021,1,1,00, 4,00)), "C"],

                                                                                # A and B both stop talking
            [pd.Timestamp(datetime.datetime(2021,1,1,00, 9,00)), "C"],          # C is still talking

            [pd.Timestamp(datetime.datetime(2021,1,1,00,14,00)), "C"],
                                                                                # A and B each time out, so are deemed "offline"
            [pd.Timestamp(datetime.datetime(2021,1,1,00,19,00)), "C"],          

            [pd.Timestamp(datetime.datetime(2021,1,1,00,24,00)), "C"],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,27,00)), "A"],          # A comes back online, after 10 minutes offline (it's been running version 1 this whole time)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,29,00)), "C"],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,32,00)), "A"],
            [pd.Timestamp(datetime.datetime(2021,1,1,00,34,00)), "C"],

            [pd.Timestamp(datetime.datetime(2021,1,1,00,37,00)), "A"],          # B comes back online, after 20 minutes offline (it was running version 1, but changed to version 2 before it went offline)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,38,00)), "B"],
            [pd.Timestamp(datetime.datetime(2021,1,1,00,39,00)), "C"],

    ]


expected_names = ["$ts", "$id", "up"]
expected_rows = [
                [pd.Timestamp("2021-01-01 00:00:00"), "A", False],
                [pd.Timestamp("2021-01-01 00:00:00"), "B", False],
                [pd.Timestamp("2021-01-01 00:00:00"), "C", False],
                [pd.Timestamp("2021-01-01 00:02:00"), "A", True],
                [pd.Timestamp("2021-01-01 00:03:00"), "B", True],
                [pd.Timestamp("2021-01-01 00:04:00"), "C", True],
                [pd.Timestamp("2021-01-01 00:17:00"), "A", False],
                [pd.Timestamp("2021-01-01 00:18:00"), "B", False],
                [pd.Timestamp("2021-01-01 00:27:00"), "A", True],
                [pd.Timestamp("2021-01-01 00:52:00"), "A", False],
                [pd.Timestamp("2021-01-01 00:54:00"), "C", False]
    ]

def timeout(df, timeout):
    gid = df.groupby("$id")
    df["version"] = gid["version"].ffill()

    delayed = df.copy(deep=True)    # Add a "potential timeout" after each event
    delayed["$ts"] += timeout

    df["count"] = gid.cumcount()    # Count each event (cumcount() produces floats which is a bit strange, but they seem to run faster than int64 - probably because of NaN detection)

    df = pd.concat([df, delayed])   # Merge in the potential timeouts
    df = df.sort_values(by="$ts", kind="mergesort")

    gid = df.groupby("$id")
    df["count"] = gid["count"].ffill()

    # Calculate timeouts
    df["time_delta"] = df['$ts'] - gid["$ts"].shift(1)    # Backward-looking
    print(df.dtypes)
    df["timer"] = df.groupby(["$id","count"])["time_delta"].cumsum()    # this group-by is slow - not the cumsum()
    df["up"] = df["timer"] <= TIMEOUT 

    # Eventify
    df["up_changed"] = df["up"] != gid["up"].shift(1) 

    df = df[df["up_changed"] == True]

    return df

def run_timeout():
    global df_big
    return timeout(df_big, TIMEOUT)


print("FUNCTIONAL TEST")
# Create data
df = pd.DataFrame(rows, columns = names)

df = timeout(df, TIMEOUT)
df = df[["$ts", "$id", "up"]].reset_index(drop=True)
print("Results\n", df)

expected = pd.DataFrame(expected_rows, columns = expected_names)
if df.equals(expected):
    print("PASSED\n")
else:
    print("FAILED\nExpected:\n", expected)
    sys.exit(-1)


print("PERFORMANCE TEST")
df_big = pd.DataFrame(rows, columns = names)
for i in range(16):
    df2 = df_big.copy()    # Create an identical dataframe later in time
    span = df2["$ts"].iloc[-1] - df2["$ts"].iloc[0]
    df2["$ts"] += span
    df_big = pd.concat([df_big, df2], ignore_index=True)    # Ignore index so we get a nice monotonic, non-duplicate index (which will make some functions much faster)
df_big['$id'] = np.random.randint(1, 100_000, df_big.shape[0])  # 100,000 IDs
df_big['$id'] = df_big['$id'].astype("category")    # IDs are arguably categorical, which improves speed. Or they are pd.StringDtype() which is slower. They are not numbers.

lprofiler = LineProfiler()
lprofiler.add_function(timeout)
lp_wrapper = lprofiler(run_timeout)
t1 = time.time()
lp_wrapper()
t2 = time.time()
lprofiler.print_stats()

print("Took {time:,}ms to do {rows:,} rows, which is {speed:,} rows/sec".format(
    time=int((t2-t1)*1000),
    rows=len(df_big),
    speed=int(len(df_big)/(t2-t1)))
)
