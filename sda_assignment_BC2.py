import os
import pandas as pd
import glob
import numpy as np
import time
import threading
import timeit

df = pd.DataFrame()


def func(rindx, cindx):
    return (cindx * np.random.rand()) + rindx


def worker(bc2_from_rindex, bc2_to_rindex, bc2_from_cindex, bc2_to_cindex, maxcols, threadnum):

    # if bc2_from_rindex > maxrows:
    #     return
    print("Thread ", threadnum, ": col block", bc2_from_cindex, "to", bc2_to_cindex, ". Rows between", bc2_from_rindex,
          "to", bc2_to_rindex, "being processed")
    for row in range(bc2_from_rindex, bc2_to_rindex):
        if row >= bc2_to_rindex:
            return
        else:
            for col in range(bc2_from_cindex, bc2_to_cindex):
                if col >= maxcols:
                    return
                else:
                    func_value = func(row, col)
                    df.iloc[row, col] = func_value


threads = []
operdir = "gas-sensor-array-temperature-modulation/"
all_files = glob.glob(os.path.join(operdir, "20160930_203718.csv"))

df = pd.concat((pd.read_csv(f, header=0) for f in all_files))

thread_num = 4
print(" \t\t###### BC2  ###### ")
df_metatuple = df.shape
nrows = df_metatuple[0]
ncols = df_metatuple[1]
print("total number of rows = ", nrows)

offset = ncols % thread_num
col_blocks = int((ncols + offset) / thread_num)
print("column blocks  = ", col_blocks)

bc2_from_row = 0
bc2_to_row = nrows

bc2_from_col = 0
bc2_to_col = col_blocks
print("Start time of processing : ", time.ctime())
start = timeit.default_timer
for i in range(1, thread_num + 1):
    t = threading.Thread(target=worker, args=(bc2_from_row, bc2_to_row, bc2_from_col, bc2_to_col, ncols, i))
    threads.append(t)
    t.start()
    time.sleep(1)
    bc2_from_col += col_blocks
    bc2_to_col += col_blocks

    if i == thread_num:
        t.join()
        break
    else:
        pass

print("All threads completed processing : ", time.ctime())
stop = timeit.default_timer()
print((stop - start) / 60, " minutes taken to complete BC2.")

df.to_csv("result_of_run_bc2.csv")


