import os
import pandas as pd
import glob
import numpy as np
import time
import timeit
import threading

df = pd.DataFrame()


def func(rindx, cindx):
    return (cindx * np.random.rand()) + rindx


def worker(bc3_from_rindex, bc3_to_rindex, bc3_from_cindex, bc3_to_cindex, maxcols, maxrows, threadnum):

    # if bc3_from_rindex > maxrows:
    #     return
    print("Thread", threadnum, ": row block", bc3_from_rindex, "to", bc3_to_rindex, "being processed")
    print("Thread", threadnum, ": col block", bc3_from_cindex, "to", bc3_to_cindex, "being processed")
    for row in range(bc3_from_rindex, bc3_to_rindex):
        if row >= maxrows:
            return
        else:
            for col in range(bc3_from_cindex, bc3_to_cindex):
                if col >= maxcols:
                    break
                else:
                    func_value = func(row, col)
                    df.iloc[row, col] = func_value


threads = []
operdir = "gas-sensor-array-temperature-modulation/"
all_files = glob.glob(os.path.join(operdir, "20160930_203718.csv"))

df = pd.concat((pd.read_csv(f, header=0) for f in all_files))

thread_num = 4
print(" \t\t###### BC3  ###### ")
df_metatuple = df.shape
nrows = df_metatuple[0]
ncols = df_metatuple[1]
# print("total number of rows = ", nrows)

offset_cols = ncols % 2
offset_rows = nrows % 2
row_blocks = int((nrows + offset_rows) / 2)
col_blocks = int((ncols + offset_cols) / 2)


bc3_from_row = 0
bc3_to_row = row_blocks

bc3_from_col = 0
bc3_to_col = col_blocks
print("Start time of processing : ", time.ctime())
start = timeit.default_timer  # time.process_time()
for i in range(1, thread_num + 1):
    t = threading.Thread(target=worker, args=(bc3_from_row, bc3_to_row, bc3_from_col, bc3_to_col, ncols, nrows, i))
    threads.append(t)
    t.start()
    time.sleep(1)
    if bc3_to_col <= ncols - 1:
        bc3_from_col += col_blocks
        bc3_to_col += col_blocks
    else:
        bc3_from_row += row_blocks
        bc3_to_row += row_blocks
        bc3_from_col = 0
        bc3_to_col = col_blocks

    if i == thread_num:
        t.join()
        break
    else:
        pass
print("All threads completed processing : ", time.ctime())
stop = timeit.default_timer()
print((stop - start) / 60, " minutes taken to complete BC3.")

df.to_csv("result_of_run_bc3.csv")


