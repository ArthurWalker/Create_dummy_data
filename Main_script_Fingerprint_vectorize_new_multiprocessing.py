import pandas as pd
import numpy as np
import tqdm
import multiprocessing as mp
import Fingerprint_functions_vectorize as fpf
import time
import os

df = pd.read_csv('./new_record3.csv',dtype='str',low_memory=True,nrows=20050)
df['HO_TEN'] = df['HO'] + ' ' + df['DEM'] + ' ' + df['TEN']
del df['Unnamed: 0']

def create_fingerprint(df):
    ho_col = df['HO_TEN'].values
    ngay_col = df['NGAY'].values
    thang_col = df['THANG'].values
    nam_col = df['NAM'].values
    raw_fingerprint = fpf.encode(ho_col, ngay_col, thang_col, nam_col)
    fingerprint = np.array([' '.join(list(np.where(i == 1)[0].astype(str))) for i in raw_fingerprint])
    df['Fingerprint'] = fingerprint
    return df


def main():
    global df
    start = time.time()
    print(time.strftime('%X %x'))
    print('RUNNING...')
    #for df in (pd.read_csv('./new_record3.csv', dtype='str', low_memory=True, chunksize=100000)):

    n_proc = mp.cpu_count()
    pool = mp.Pool(processes=n_proc)
    proc_results = pool.map(create_fingerprint, [d for d in np.array_split(df,n_proc)])
    pool.close()
    results = pd.concat(proc_results)

    if not os.path.isfile('./fingerprint_vectorize3.csv'):
        results.to_csv('./fingerprint_vectorize3.csv')
    else:
        results.to_csv('./fingerprint_vectorize3.csv', mode='a', header=False, index_label=False)
    print(time.time() - start)
    print(time.strftime('%X %x'))


if __name__ == '__main__':
    main()

