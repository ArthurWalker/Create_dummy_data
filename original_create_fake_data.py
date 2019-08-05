import numpy as np
import pandas as pd
import time
import random
from tqdm import tqdm
from multiprocessing import Pool

def get_random_data(df):
    ho = random.choice(list(df['HO']))
    dem =random.choice(list(df['DEM']))
    ten = random.choice(list(df['TEN']))
    ngay=random.randrange(1,32)
    thang =random.randrange(1,13)
    nam =random.randrange(1901,2019)
    sex = random.randrange(0,2)
    tinhKS='{:02d}TTT'.format(random.randrange(1,65))
    huyenKS='{:003d}HH'.format(random.randrange(0,260))
    xaKS='{:00005d}'.format(random.randrange(0,20000))
    new_row = {'MA_XA_KS':xaKS,'MA_HUYEN_KS':huyenKS,'MA_TINH_KS':tinhKS,'GIOI_TINH':sex,'HO':ho,'DEM':dem,'TEN':ten,'NGAY':ngay,'THANG':thang,'NAM':nam}
    return new_row


def main():
    start = time.time()
    df = pd.read_csv('./fixed_data.csv',dtype='str',index_col=False)
    del df['Unnamed: 0']
    pool = Pool(processes=4)
    results = pool.map(get_random_data, [df for i in range(1000000)])
    pool.close()

    df_result = pd.DataFrame(results)
    print ('Join data')
    joined_df = pd.concat([df,df_result],ignore_index=True,sort=False)
    joined_df.to_csv('./new_record.csv')
    print (time.time()-start)

if __name__=='__main__':
    main()