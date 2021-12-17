# Note to Mentor
# --------------
# You don't need to understand all of the following, it's mainly for a bit of context
# Let's initially just try to make it work at all, and then we can look at efficiency
#
# We call our dataset a "pointfile". It is a collection of rows of timestamped data which comes from multiple devices, over some period of time.
#
# Every message (row) in our dataset contains at least these columns:
#     $ts: the timestamp
#     $id: the device ID
#     (optionally other properties that have changed)
#
# If several devices each send a message at the same time, these are separate rows, which have the same timestamp but different ids
# In this test case we have just one other property - "version", which is guaranteed to a) change slowly and b) have only a few possible values.
# 
# In English, this canonical query is:
#  "Uptime by software version, grouped by time"
# 
# This requires roughly the following steps:
# 1. Work out when each device is online and offline. If a device hasn't been heard for a while, it is deemed to be offline
# 2. Calculate the uptime, which is "percentage of time online" for each device individually
# 3. Group by software version, which may change per device during the period of the analysis
# 4. Group into time bins
#
# So the result will be something like:
#  TIME BIN:            0       1       2  
#  Version 1 uptime:    100%    85%     34%
#  Version 2 uptime:    99%     0%      3%
# 
# Uptime
# ------
# We define uptime "optimistically", i.e. we deem the device to still be online during the period of inactivity before its timeout expires
# Because we are doing STREAMING analytics, we have the concept of global "event time", which is the latest timestamp seen on ANY device.
# This is the sweeping "now" against which things like timeouts are determined.
# So if a device falls silent, it is the timestamps sent by *other* devices which advance "event time", and cause the timeout.
# The uptime is normalised by a denominator which is the number of devices on that version for any given time period.
# The input file is guaranteed to be sorted by ascending time.
#
# From the above, to calculate timeouts, it might appear that we have to sweep through the events, tracking each device individually, yet against the global event time.
# HOWEVER, we are reading from a Parquet file which contains metadata which allows us to determine the last time in the file without scanning.
# Therefore we know the period over which we need to consider the timeout behaviour of each device, and since devices are independent (for timeouts) then we can consider each independently.
#
# Keyframes
# ---------
# The way that state is carried from one pointfile to the next is via a "keyframe", which is the first row in the data.
# The keyframe has a value set for EVERY known device and EVERY known property.
# So even if, in this file, a device doesn't talk at all, or doesn't emit certain properties, all its properties WILL appear in the keyframe
# We don't know what happens after the last row, so can't emit any timestamps after that
# So when calculating timeout we ignore the last row (because it will appear again in the keyframe of the next file and get dealt with there)
#
# Note: DevicePilot actually maintains timestamps for every property, not just per-device, but that's an obvious extension so we don't have to model it here.
# 
# 

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
# print(df)

for name, g in df.groupby("$id"):
    if name == "A":
        print("Group",name, ":")
        g['timer'] = TIMEOUT            # Set timer on every message
        print("Data with timer:\n",g)

        # Create time bins
        bins = pd.DataFrame({'$ts' : pd.bdate_range('2021-01-01', freq='5min', periods = 10).tolist(), 'timer' : pd.Timedelta(0)})
        bins.set_index('$ts')

        # Merge bins with existing events (can we use merge(), to avoid sort()? Everything already sorted)
        g = pd.concat([g,bins])
        g = g.sort_values(by="$ts")

        cols_to_drag = ["$id", "version"]
        g.loc[:,cols_to_drag] = g.loc[:,cols_to_drag].ffill()

        print("Data with time bins:\n", g)

        g['time_delta'] = g['$ts'].diff() 
        # g['time_delta'].iloc[0] = pd.Timedelta(0)  # First element will be NaT otherwise
        print("g now:\n",g)

        g['prev_delta'] = g['time_delta'].shift(1)
        g['D'] = np.where(g.timer, g.timer, g.time_delta)
        print("g now:\n",g)

        print("Result:\n",g)

        # g.assign(timer = TIMEOUT)  # Start the timer on every message
        # g['timeout'] = g['diff'] > TIMEOUT
        # g['downtime'] = g['diff'] - TIMEOUT
        # g.loc[g['timeout'], "d2"] = g['downtime']
        # ts = g.resample("15T")
