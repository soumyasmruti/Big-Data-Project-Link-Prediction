
# coding: utf-8

# In[ ]:

import math, os, snap, json

class IdsAsInt():
    def __init__(self):
        self.x = -1
        self.n = {}

    def __getitem__(self, key):
        if key not in self.n:
            self.x += 1
            self.n[key] = self.x
        return self.n[key]

    
def get_degree(g):
    degree, distDeg = dict(), dict()
    sum = 0
    for n in g.Nodes():
        degree[n.GetId()] = n.GetOutDeg()
    
    for nd in degree:
        sum += 1
        if degree[nd] not in distDeg: distDeg[degree[nd]] = 1
        else: distDeg[degree[nd]] += 1
    
    with open('graph_degree_dist.txt', 'w') as f:
        for ndd in distDeg:
            norm = float(distDeg[ndd])/float(sum)
            f.write(str(ndd) + "\t" + str(norm) + '\n')
        
        
def create_graph(out_dir):
    
    nid = IdsAsInt()

    with open("./data/created/reviews.json") as f:
        reviews_it = json.load(f)

    nids = set()
    for key, review in reviews_it.iteritems():
        nids.add(nid['u'+review['Author']])
        nids.add(nid['h'+review['HotelID']])

    with open(out_dir + 'graph.txt', 'w') as graph:
        for key, review in reviews_it.iteritems():
            u = nid['u'+review['Author']]
            h = nid['h'+review['HotelID']]
            if u in nids and h in nids:
                graph.write("{:} {:}\n".format(u, h))

                   
def avg_deg(g):
    dic_fre = dict()
    ec = 0
    nc = 0
    summ = 0
    num = 0
    xmin = 4
    
    for n in g.Nodes():
        ec += n.GetOutDeg()
        nc += 1
    print "Average degree of a node: %f" % (float(ec)/float(nc))

def is_connected(g):
    tiv = snap.TIntPrV()
    snap.GetWccSzCnt(g, tiv)
    for c in tiv:
        print "Size: %d - Number of Components: %d" % (c.GetVal1(), c.GetVal2())
    
def statistics(out_dir):
    g = snap.LoadEdgeList(snap.PNGraph,out_dir + 'graph.txt',0,1,' ')
    print "Graph has %d nodes" % g.GetNodes()
    print "Graph has %d edges" % g.GetEdges()
    print "Graph is connected? %s" % snap.IsConnected(g)
    get_degree(g)
    print "Diameter is: %f" % snap.GetBfsFullDiam(g, 200, False)
    avg_deg(g)
    is_connected(g)

if __name__ == '__main__':
    create_graph('./data/stats/')
    statistics('./data/stats/')


# In[ ]:



