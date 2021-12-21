import pandas as pd
import numpy as np
import datetime

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

df = pd.DataFrame(rows, columns = names)
df.set_index("$ts")

group = df.groupby("$id")
df["version"] = group["version"].ffill()
df["count"] = group.cumcount()  # Every group gets a rising index

print("A\n", df.groupby("$id").get_group("A"))

delayed = df.copy(deep=True)
delayed["$ts"] += TIMEOUT
delayed = delayed.drop(columns=["count"])

print("delayed\n", delayed.groupby("$id").get_group("A"))

df = pd.concat([df, delayed])
df = df.sort_values(by="$ts", kind="mergesort")
df["count"] = df.groupby("$id")["count"].ffill()

df["time_delta"] = df['$ts'] - df.groupby("$id")["$ts"].shift(1)    # Backward-looking

df["timer"] = df.groupby(["$id","count"])["time_delta"].cumsum()
df["up"] = df["timer"] <= TIMEOUT 

# Eventify
df["up_changed"] = df["up"] != df.groupby("$id")["up"].shift(1) 

df = df[df["up_changed"] == True]

print("A\n", df.groupby("$id").get_group("A"))
print("B\n", df.groupby("$id").get_group("B"))
print("C\n", df.groupby("$id").get_group("C"))
