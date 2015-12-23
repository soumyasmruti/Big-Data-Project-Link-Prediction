
# coding: utf-8

# In[ ]:

import snap, datetime, json
from collections import Counter, defaultdict 
import random

def convert_date(dic):
    date = dic["Date"]
    return datetime.date(*map(int, date.split(' ')[0].split("-")))

def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))

def load_json(fname):
    with open(fname) as f:
        return json.load(f)
    
def iterator(itr):
    for elem in itr:
        yield elem
        
def matrix_creator(inpdir, num_users=0, mind=1, min_active_time=None, edge_only=False):
   
    active_now = []
    users = []
    users_rest = []
    
    G = snap.LoadEdgeList(snap.PUNGraph, inpdir + 'graph.txt', 0, 1)
    with open(inpdir + 'edges.txt') as f:
        edges = {tuple(map(int, line.split())) for line in f}
    
    edge_count = Counter()
    for (u, h) in edges:
        edge_count[u] += 1
        
    review_it = load_json(inpdir + 'review.json')
    num_hotels = len(load_json(inpdir + "hotels.json"))
    
    for Node in iterator(G.Nodes()):
        u = Node.GetId()
        if edge_only and not u in edge_count: continue
        if str(u) not in review_it or Node.GetOutDeg() < mind: continue
        if min_active_time:
            reviewed_now = False
            for b in review_it[str(u)]:
                if (int(u), int(b)) in edges: continue
                for r in review_it[str(u)][b]:
                    if convert_date(r) > min_active_time:
                        users.append(u)
                        active_now.append(u)
                        reviewed_now = True
                        break
                if reviewed_now: break
            if not reviewed_now: users_rest.append(u)
        else:
            users.append(u)

    if min_active_time:
        rp = sum(edge_count[u] for u in active_now)
        re = len(active_now) * num_hotels
        op = sum(edge_count[u] for u in users_rest)
        oe = len(users_rest) * num_hotels
        
    random.seed(0)
    users = random.sample(users, num_users)

    matrix = defaultdict(dict)
    for u in iterator(users):
        ch = snap.TIntV()
        snap.GetNodesAtHop(G, u, 3, ch, True)
        for h in ch:
            if random.random() < 0.01:
                matrix[u][h] = 0
            elif (u, h) in edges:
                matrix[u][h] = 1

    pos_3hop = 0
    pos_3hop = sum([matrix[u][h] for u in matrix for h in matrix[u]])
    exmp_3hop = sum(len(matrix[u]) for u in matrix)
    np = sum([edge_count[u] for u in users])
    ne = len(users) * num_hotels
    write_json(matrix, inpdir + 'matrix.json')
    
if __name__ == '__main__':
    matrix_creator('./data/train/', num_users=10000, mind=1, min_active_time=datetime.date(2009, 1, 1), edge_only=False)
    matrix_creator('./data/test/', num_users=10000, mind=1, min_active_time=datetime.date(2010, 1, 1), edge_only=False)


# In[ ]:



