from hashlib import sha1, md5
import numpy as np


def get_fingerprint(str_value, fingerprint_length=300, num_hash_funcs=128, value_type='fullname'):
    str_value = str(str_value).lower()
    #str_value = str_value.str.lower()
    if value_type == 'fullname':
        str_value = '_' + str_value + '_'
        #bigrams = bigrams_f(str_value)
        bigrams = [str_value[i:i + 2] for i in range(len(str_value) - 1)]
    elif value_type == 'dob':
        bigrams = [f'{i}{j}' for (i, j) in enumerate(str_value)]

    indices = []
    fingerprint = np.zeros(fingerprint_length)
    for gram in bigrams:
        for i in range(num_hash_funcs):
            encoder_1 = sha1()
            encoder_2 = md5()
            encoder_1.update(gram.encode('utf8'))
            encoder_2.update(gram.encode('utf8'))
            encoded_1 = int(encoder_1.hexdigest(), 16)
            encoded_2 = int(encoder_2.hexdigest(), 16)
            idx = (encoded_1 + i * encoded_2) % fingerprint_length
            indices.append(idx)
    indices = list(set(indices))
    fingerprint[indices] = 1
    return fingerprint.astype(int)

def encode(record):
    fullname_vec = get_fingerprint(record['HO_TEN'], value_type='fullname', num_hash_funcs=16)

    day_vec = get_fingerprint(record['NGAY'], value_type='dob', num_hash_funcs=8)
    month_vec = get_fingerprint(record['THANG'], value_type='dob', num_hash_funcs=8)

    year_vec = get_fingerprint(record['NAM'], value_type='dob', num_hash_funcs=8)
    res = (fullname_vec | day_vec | month_vec | year_vec).astype(int)
    return res

