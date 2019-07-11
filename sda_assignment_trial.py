import os
import pandas as pd
import glob
import numpy as np
import time
import threading

df = pd.DataFrame()


def func(rindx, cindx):
    return (cindx * np.random.rand()) + rindx


def worker(bc1_from_rindex, bc1_to_rindex, bc1_from_cindex, bc1_to_cindex, maxrows):

    # if bc1_from_rindex > maxrows:
    #     return
    print("Row block", bc1_from_rindex, "to", bc1_to_rindex, "being processed")
    for row in range(bc1_from_rindex, bc1_to_rindex):
        if row >= maxrows:
            return
        else:
            for col in range(bc1_from_cindex, bc1_to_cindex):
                func_value = func(row, col)
                df.iloc[row, col] = func_value


threads = []
operdir = "gas-sensor-array-temperature-modulation/"
all_files = glob.glob(os.path.join(operdir, "20160930_203718.csv"))

df = pd.concat((pd.read_csv(f, header=0) for f in all_files))

thread_num = 4
print(" \t\t###### BC1  ###### ")
df_metatuple = df.shape
nrows = df_metatuple[0]
ncols = df_metatuple[1]
print("total number of rows = ", nrows)

offset = nrows % thread_num
row_blocks = int((nrows + offset) / thread_num)
print("row_blocks  = ", row_blocks)

bc1_from_row = 0
bc1_to_row = row_blocks

bc1_from_col = 0
bc1_to_col = ncols

start = time.process_time()
for i in range(1, thread_num + 1):
    t = threading.Thread(target=worker, args=(bc1_from_row, bc1_to_row, bc1_from_col, bc1_to_col, nrows))
    threads.append(t)
    t.start()

    bc1_from_row += row_blocks
    bc1_to_row += row_blocks

    if i == thread_num:
        t.join()
        break
    else:
        pass

print((time.process_time() - start) / 60, " minutes taken to complete BC1.")

df.to_csv("result_of_run.csv")
bc1_from_row += row_blocks
bc1_to_row += row_blocks

