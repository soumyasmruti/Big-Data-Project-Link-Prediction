
# coding: utf-8

# In[1]:

import snap, datetime, json

def load_json(fname):
    with open(fname) as f:
        return json.load(f)
    
def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))

def hopper(nodeid, graph):
    hop_n = snap.TIntV()
    snap.GetNodesAtHop(graph, nodeid, 2, hop_n, True)
    del graph
    return {hop for hop in hop_n}  

def cal_hop2_dis(nodes, matrix, graph, checkstr, nodes_2hop):
    hops = {}
    for i in matrix:
        if checkstr == "users":
            nodeid = int(i)
            if nodeid in nodes:
                hops[nodeid] = hopper(nodeid, graph)
        else:
            for j in matrix[i]:
                if int(j) not in nodes_2hop:
                    nodeid = int(j)
                    if nodeid in nodes:
                        hops[nodeid] = hopper(nodeid, graph)
    del nodes, matrix, graph, nodes_2hop
    return hops
            
def get_hotels_neighbor(nodes, matrix, graph):
    neighbors = {}
    for i in matrix:
        for j in matrix[i]:
            if int(j) not in neighbors and int(j) in nodes:
                temp = snap.TIntV()
                snap.GetNodesAtHop(graph, int(j), 1, temp, True)
                neighbors[int(j)] = {t for t in temp}
    del nodes, matrix, graph
    return neighbors
    
def get_user_neighbor(nodes, matrix, graph):
    neighbors = {}
    for i in matrix:
        if int(i) not in neighbors and int(i) in nodes:
            temp = snap.TIntV()
            snap.GetNodesAtHop(graph, int(i), 1, temp, True)
            neighbors[int(i)] = {t for t in temp}
            
    del nodes, matrix, graph
    return neighbors


def cal_similarity(matrix, s, f, nodes_2hop, neighbors, nodes):
    mat = {}
    for i in matrix:
            for j in matrix[i]:
                if int(i) in nodes and int(j) in nodes:
                    if s == 'jaccard':
                        mat[i][j] = jaccard(nodes_2hop[int(j)], neighbors[int(j)])
                    elif s == 'cn' :
                        mat[i][j] = cn(nodes_2hop[int(i)], neighbors[int(j)])
                else:
                    mat[i][j]=0 
    return mat

def jaccard(a, b):
    return float(len(a.intersection(b)))/float(len(a.union(b)))

def cn(a, b):
    return len(a.intersection(b))

def hotels(matrix, graph, sims, out):
    print "Hotels"
    nodes=[N.GetId() for N in snap.Nodes(graph)]
    nodes_2hop =  {}
    print "Here hotels"
    nodes_2hop = cal_hop2_dis(nodes, matrix, graph, "hotel", nodes_2hop)
    print "Here hotels Neigh"
    neighbors = get_user_neighbor(nodes, matrix, graph)

    for s, f in zip(sims,out):
        user_sim = cal_similarity(matrix, s, f, nodes_2hop, neighbors, nodes)
        write_json(user_sim, f)

def users(matrix, graph, sims, out):
    print "Users"
    nodes=[N.GetId() for N in snap.Nodes(graph)]
    nodes_2hop = {}
    print "Here User"
    nodes_2hop = cal_hop2_dis(nodes, matrix, graph, "users", nodes_2hop)
    print "Here user Neigh"
    neighbors = get_hotels_neighbor(nodes, matrix, graph)
    for s, f in zip(sims,out):
        user_sim = cal_similarity(matrix, s, f, nodes_2hop, neighbors, nodes)
        write_json(user_sim, f)

        
def caller(matrix, graph, usims, uout, hsims, hout):
    start = datetime.datetime.now()
    matrix = load_json(matrix)
    graph = snap.LoadEdgeList(snap.PUNGraph, graph, 0, 1)
    hotels(matrix, graph, hsims, hout)
    users(matrix, graph, usims, uout)
    
    
if __name__ == '__main__':
    train = "./data/train/"
    caller(train+'matrix.json', train+'graph.txt', ['jaccard', 'cn'],
         [train + "user_cn.json,", train+'user_jac.json'], ['jaccard', 'cn'],
         [train + 'hotel_cn.json', train+'hotel_jac.json'])
    test = "./data/test/"
    caller(test+'matrix.json', test+'graph.txt', ['jaccard', 'cn'],
         [test+'user_cn.json', test+'user_jac.json'], ['jaccard', 'cn'],
         [test+'hotel_cn.json', test+'hotel_jac.json'],)
   


# In[ ]:



