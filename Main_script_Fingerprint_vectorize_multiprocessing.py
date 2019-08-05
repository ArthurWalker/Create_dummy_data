import pandas as pd
import numpy as np
import tqdm
import multiprocessing as mp
import Fingerprint_functions_vectorize as fpf
import time
import os

def create_fingerprint(df):
    ho_col = df['HO_TEN'].values
    ngay_col = df['NGAY'].values
    thang_col = df['THANG'].values
    nam_col = df['NAM'].values
    raw_fingerprint = fpf.encode(ho_col,ngay_col,thang_col,nam_col)
    fingerprint = np.array([' '.join(list(np.where(i == 1)[0].astype(str))) for i in raw_fingerprint])
    df['Fingerprint']=fingerprint
    return df

def process_chunk(df):
    chunk_res = create_fingerprint(df)
    return chunk_res

def main():
    global df
    start=time.time()
    print(time.strftime('%X %x'))
    print('RUNNING...')
    #for df in (pd.read_csv('./fixed_data.csv', dtype='str',low_memory=True, chunksize=200)):
    df = pd.read_csv('./new_record3.csv',dtype='str',low_memory=True,nrows=10000)
    df['HO_TEN']=df['HO']+' '+df['DEM']+' '+df['TEN']

    n_proc=mp.cpu_count()
    chunk_size=len(df)//n_proc
    proc_chunks= []
    for i_proc in range(n_proc):
        chunkstart=i_proc*chunk_size
        chunkend=(i_proc+1)*chunk_size if i_proc < n_proc-1 else None
        proc_chunks.append(df.iloc[slice(chunkstart,chunkend)])
    with mp.Pool(processes=n_proc) as pool:
        proc_results = [pool.apply_async(process_chunk, args=(chunk,)) for chunk in proc_chunks]
        result_chunks = [r.get() for r in proc_results]
    results = pd.concat(result_chunks)

    if not os.path.isfile('./fingerprint_vectorize3.csv'):
        results.to_csv('./fingerprint_vectorize3.csv')
    else:
        results.to_csv('./fingerprint_vectorize3.csv',mode='a',header=False,index_label=False)
    print (time.time()-start)
    print(time.strftime('%X %x'))
if __name__=='__main__':
    main()

