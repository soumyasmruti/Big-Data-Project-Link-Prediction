
# coding: utf-8

# In[4]:

import json
import numpy as np
from scipy import sparse as sp
from scipy.sparse.linalg import svds

def load_json(fname):
    with open(fname) as f:
        return json.load(f)
    
def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))

def calculate_svd(out, k=75):
    users = load_json('./data/' + out + '/user.json').keys()
    hotels = load_json('./data/' + out + '/hotels.json').keys()
    matrix = load_json('./data/' + out + '/matrix.json')

    row = dict(zip(users, range(len(users))))
    col = dict(zip(hotels, range(len(hotels))))

    uh_matrix = sp.lil_matrix((len(users), len(hotels)), dtype=float)
    with open('./data/' + out + '/graph.txt') as f:
        for line in f:
            i, j = line.split()
            uh_matrix[row[i], col[j]] = 1
    uh_matrix = sp.csr_matrix(uh_matrix)
    
    u, s, vt = svds(uh_matrix, k=k)
    us = u * s

    for i in matrix:
        for j in matrix[i]:
            matrix[i][j] = np.dot(us[row[i], :], vt[:, col[j]])
    write_json(matrix, './data/' + out + '/svd.json')
    
    
if __name__ == '__main__':
    calculate_svd('train')
    calculate_svd('test')


# In[ ]:



