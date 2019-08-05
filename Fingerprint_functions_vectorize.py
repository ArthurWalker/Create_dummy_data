from hashlib import sha1, md5
import numpy as np

def fc(gram,fingerprint_length,i):
    encoder_1 = sha1()
    encoder_2 = md5()
    encoder_1.update(gram.encode('utf8'))
    encoder_2.update(gram.encode('utf8'))
    encoded_1 = int(encoder_1.hexdigest(), 16)
    encoded_2 = int(encoder_2.hexdigest(), 16)
    idx = (encoded_1 + i * encoded_2) % fingerprint_length
    return idx

def get_fingerprint(str_value, fingerprint_length=300, num_hash_funcs=128, value_type='fullname'):
    str_value = np.array([x.lower() if isinstance(x, str) else x for x in str_value])
    if value_type == 'fullname':
        str_value = np.array(['_'+x+'_' for x in str_value])
        #bigrams = bigrams_f(str_value)
        bigrams = np.array([[x[i:i+2] for i in range(len(x)-1)] for x in str_value])
    elif value_type == 'dob':
        bigrams = np.array([[f'{i}{j}' for (i, j) in enumerate(x)] for x in str_value])

    np_arr =[]
    for bigram in bigrams:
        indices = []
        fingerprint = np.zeros(fingerprint_length)
        #indices = [[fc(gram,fingerprint_length,indices,i) for i in range(num_hash_funcs)] for gram in bigram]
        for gram in bigram:
            for i in range(num_hash_funcs):
                idx = fc(gram,fingerprint_length,i)
                indices.append(idx)
        indices = list(set(indices))
        fingerprint[indices] = 1
        np_arr.append(fingerprint.astype(int))
    return np.array(np_arr)

def encode(ho_col,ngay_col,thang_col,nam_col):
    fullname_vec = get_fingerprint(ho_col, value_type='fullname', num_hash_funcs=16)

    day_vec = get_fingerprint(ngay_col, value_type='dob', num_hash_funcs=8)
    month_vec = get_fingerprint(thang_col, value_type='dob', num_hash_funcs=8)

    year_vec = get_fingerprint(nam_col, value_type='dob', num_hash_funcs=8)
    res = (fullname_vec | day_vec | month_vec | year_vec).astype(int)
    return res

