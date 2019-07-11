import os
import pandas as pd
import glob
import numpy as np
import time
import threading

df = pd.DataFrame()


def func(rindx, cindx):
    return (cindx * np.random.rand()) + rindx


def worker(bc1_from_rindex, bc1_to_rindex, bc1_from_cindex, bc1_to_cindex, maxcols):

    # if bc1_from_rindex > maxrows:
    #     return
    print("col block", bc1_from_cindex, "to", bc1_to_cindex, "being processed")
    for row in range(bc1_from_rindex, bc1_to_rindex):
        if row >= bc1_to_rindex:
            return
        else:
            for col in range(bc1_from_cindex, bc1_to_cindex):
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

bc1_from_row = 0
bc1_to_row = nrows

bc1_from_col = 0
bc1_to_col = col_blocks

start = time.process_time()
for i in range(1, thread_num + 1):
    t = threading.Thread(target=worker, args=(bc1_from_row, bc1_to_row, bc1_from_col, bc1_to_col, ncols))
    threads.append(t)
    t.start()

    bc1_from_col += col_blocks
    bc1_to_col += col_blocks

    if i == thread_num:
        t.join()
        break
    else:
        pass

print((time.process_time() - start) / 60, " minutes taken to complete BC2.")

df.to_csv("result_of_run_bc2.csv")


