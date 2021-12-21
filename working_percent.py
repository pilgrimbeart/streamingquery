# percent.py
# Given a stream of timestamped boolean values (e.g. "up" measures)
# ...calculate the percentage time when each is true

import pandas as pd
import numpy as np
import time
import datetime
import sys

BINSIZE = pd.Timedelta(minutes=5)   # What size time bins do we want on the output?

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

            [pd.Timestamp(datetime.datetime(2021,1,1,00,37,00)), "A", 1],          # B comes back online, after 20 minutes offline (it was running version 1, but changed to version 2 before it went offline)
            [pd.Timestamp(datetime.datetime(2021,1,1,00,38,00)), "B", 1],
            [pd.Timestamp(datetime.datetime(2021,1,1,00,39,00)), "C", 1],

    ]

def percent_of_time_online(df, bins):
    for name, g in df.groupby("$id"):   # Pass sort=false for faster
        if name == "A":
            print("Group",name, ":")
    
            # Merge bins with existing events (can we use merge(), to avoid sort()? Everything already sorted)
            g = pd.concat([g, bins])
            g = g.sort_values(by="$ts", kind="mergesort")

            print("Merged bins\n",g)
    
            cols_to_drag = ["$id", "version", "up", "bin_number"]
            g.loc[:,cols_to_drag] = g.loc[:,cols_to_drag].ffill()
    
            g['time_delta'] = g['$ts'].diff().shift(-1)     # Forward-looking deltas
    
            g['up_time'] = g['time_delta'] * g['up']
    
            print("g:\n",g)
    
            result = g.groupby(['$id', 'bin_number'])['up_time'].sum()
            
            return result

df = pd.DataFrame(rows, columns = names)
df.set_index("$ts")

# Create time bins
NUM_BINS = 10
bins = pd.DataFrame({'$ts' : pd.bdate_range('2021-01-01', freq='5min', periods = NUM_BINS).tolist(), 'bin_number' : range(NUM_BINS)})
bins.set_index('$ts')

result = percent_of_time_online(df, bins)
print(result)

sys.exit(0)

df_big = df
for power in range(16):
    df_big = pd.concat([df_big, df_big])
    print("df_big",len(df_big),"rows")

t1 = time.time()
result = percent_of_time_online(df_big, bins)
t2 = time.time()
print("Took",t2-t1)
