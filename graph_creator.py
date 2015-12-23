
# coding: utf-8

# In[10]:

import json, datetime
import os
import random
import snap
from collections import defaultdict, Counter

def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))
        
def load_json(fname):
    with open(fname) as f:
        return json.load(f)

class IdsAsInt():
    def __init__(self):
        self.x = -1
        self.n = {}

    def __getitem__(self, key):
        if key not in self.n:
            self.x += 1
            self.n[key] = self.x
        return self.n[key]

def convert_date(dic):
    date = dic["Date"]
    return datetime.date(*map(int, date.split(' ')[0].split("-")))

def create_final_json(n_id, nids, inp, out):
    return write_json({n_id(x): y for x, y in load_json(inp).iteritems() if n_id(x) in nids}, out)

def make_graph(t1, t2, out):
    
    nid = IdsAsInt()
    reviews_it = {}
    
    with open("./data/created/reviews.json") as f:
        reviews_it = json.load(f)

    nids = set()
    for key, review in reviews_it.iteritems():
        if convert_date(review) < t1:
            nids.add(nid['u' + review['Author']])
            nids.add(nid['h' + review['HotelID']])

    create_final_json(lambda ud: nid['u' + ud], nids, './data/created/users.json', out + 'user.json')

    create_final_json(lambda hd: nid['h' + hd], nids, './data/created/hotels.json', out+'hotels.json')

    with open(out+'graph.txt', 'w') as graph, open(out+'edges.txt', 'w') as edges:
        review_data = defaultdict(lambda: defaultdict(list))
        for key, review in reviews_it.iteritems():
            ukey = nid['u' + review['Author']]
            hkey = nid['h' + review['HotelID']]
            if ukey in nids and hkey in nids:
                date = convert_date(review)
                if date < t1:
                    review_data[ukey][hkey].append(review)
                    graph.write("{:} {:}\n".format(ukey, hkey))
                elif date < t2:
                    edges.write("{:} {:}\n".format(ukey, hkey))

        for r in review_data:
            for ur in review_data[r]:
                review_data[r][ur] = sorted(review_data[r][ur], key=convert_date, reverse=True)

        write_json(review_data, out+"review.json")
        
if __name__ == '__main__':
    make_graph(datetime.date(2010, 2, 15), datetime.date(2010, 9, 15), './data/train/')
    make_graph(datetime.date(2012, 2, 15), datetime.date(2012, 9, 15), './data/test/')
    


# In[ ]:



