import pandas as pd
import Fingerprint_functions_no_vectorize as fpf
import numpy as np
from multiprocessing import Pool
import time
import os

def create_fingerprint(data):
    raw_fingerprint = fpf.encode(data)
    fingerprint = ' '.join(list(np.where(raw_fingerprint == 1)[0].astype(str)))
    data['Fingerprint']=fingerprint
    return data

def main():
    start=time.time()
    print(time.strftime('%X %x'))
    print('RUNNING...')

    pool = Pool(processes=4)
    #for df in (pd.read_csv('./new_record3.csv',dtype='str',low_memory=True,chunksize=10000)):
    df = pd.read_csv('./new_record3.csv',dtype='str',low_memory=True,nrows=20050)
    df['HO_TEN']=df['HO']+' '+df['DEM']+' '+df['TEN']
    # del df['HO']
    # del df['DEM']
    # del df['TEN']
    records = df.to_dict('records')

    #fingerprints = []
    results = pool.map(create_fingerprint,[record for record in records])
    df_fg = pd.DataFrame(results )
    pool.close()
    if not os.path.isfile('./fingerprint3.csv'):
        df_fg.to_csv('./fingerprint3.csv')
    else:
        df_fg.to_csv('./fingerprint3.csv',mode='a',header=False,index_label=False)
    print (time.time()-start)
    print(time.strftime('%X %x'))
if __name__=='__main__':
    main()

