
# coding: utf-8

# In[1]:

from pyspark import SparkContext, SparkConf
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

import util, datetime, json
import networkx as net
import numpy as np
from scipy import sparse


# In[ ]:

def load_json(fname):
    with open(fname) as f:
        return json.load(f)
    
def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))
        
def convert_date(dic):
    date = dic["Date"]
    return datetime.date(*map(int, date.split(' ')[0].split("-")))

def iterator(itr):
    for elem in itr:
        yield elem

block_size = 4
block_bool = False
def fill_block(B, x):
    B[x[0]%block_size, x[1]%block_size] = x[2] 
    return B

def random_walker(typeVar, date, bool_edge=False):
    graph = net.read_edgelist('./data/' + typeVar + '/graph.txt', nodetype=int)
    matrix = load_json('./data/' + typeVar + '/matrix.json')
    if bool_edge:
        reviews = load_json('./data/' + typeVar + '/review.json')
        edges = graph.edges()
        for e in iterator(edges):
            i, j = str(e[0]), str(e[1])
            if i not in reviews or j not in reviews[n1]:
                i, j = j, i
            graph[e[0]][e[1]]['weight'] = 1.0 / ((date - convert_date(reviews[i][j][0])).days + 180)
        del reviews 
        
    adj_mat = net.adjacency_matrix(graph)
    inv_deg_mat = sparse.diags([[1.0 / adj_mat.getrow(i).sum() for i in range(adj_mat.shape[0])]], [0])
    tra_mat = inv_deg_mat.dot(adj_mat)

    if block_bool:
        inv_deg_mat = sc.parallelize(inv_deg_mat)
        adj_mat = sc.parallelize(adj_mat)
        a = inv_deg_mat.map( lambda s:((int(s[0])/block_size, (int(s[1]))/block_size), (int(s[0]), int(s[1]), float(s[2]))))              .aggregateByKey(np.matrix(np.zeros((block_size, block_size))), fill_block, add).cache()

        b = adj_mat.map(lambda s: ( (int(s[0])/block_size, (int(s[1]))/block_size), (int(s[0]), int(s[1]), float(s[2]))))              .aggregateByKey(np.matrix(np.zeros((block_size, block_size))), fill_block, add).cache()
        
        a1 = a.map(lambda (k,v): (k[1], (k[0],v)))
        b1 = b.map(lambda (k,v): (k[0], (k[1],v)))
        c = a1.join(b1).map(lambda (k,v): ((v[0][0], v[1][0]), v[0][1]*v[1][1])).reduceByKey(add) 
        sorted(c.collect())
    
    for u in iterator(matrix):
        i = int(u)
        p = np.zeros(tra_mat.shape[0])
        p[i] = 1.0
        p = sparse.csr_matrix(p)
        while 0 < i < 10:
            p = np.dot(p, tra_mat)
            p *= 0.8
            p[0, i] += 0.2
        p = p.todense()
        for j in matrix[u]:
            matrix[u][j] = p[0, int(j)]

    write_json(matrix, './data/' + typeVar + ('/random_walks_by_weight.json' if bool_edge else '/random_walks.json'))


if __name__ == '__main__':
    random_walker('train', datetime.date(2011, 2, 15), False)
    random_walker('test', datetime.date(2012, 2, 15), True)


# In[ ]:



