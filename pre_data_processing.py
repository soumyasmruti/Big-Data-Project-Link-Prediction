
# coding: utf-8

# In[2]:

import glob, json
import dateutil.parser
from collections import Counter

def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))

def make_init_user_hotels(reviews_it):
    userid_list = []
    hotelId_list = []
    user_date = {}
    for review, key in reviews_it.iteritems():
        try:
            user_date[key["Author"]].append(key["Date"])
        except:
            user_date[key["Author"]] = []
        userid_list.append(key["Author"])
        hotelId_list.append(key["HotelID"])
    u_c = Counter(userid_list)
    h_c = Counter(hotelId_list)
    return u_c, h_c, user_date
#     write_json(u_c, "int_users.json")
#     write_json(h_c, "int_hotels.json")
#     write_json(user_date, "user_date.json")

def make_user_list(review_json, user_json, date_json, hotel_json):
    user_loc = {}
    hotels = {}
    for review, key in review_json.iteritems():
        userid = key["Author"]
        user_loc[userid] = {"ReviewCount": user_json[userid], 
                            "LatestDate": max(date_json[userid]) if date_json[userid] else -1,
                            "UserLocation": key["Author Location"]}
        
        hotelid = key["HotelID"]
        hotels[hotelid] = {"ReviewCount": hotel_json[hotelid],
                           "HotelLocation": key["HotelLocation"]}
    
    write_json(user_loc, "./data/created/users.json")
    write_json(hotels, "./data/created/hotels.json")    

    
def main_creator():
    reviews_it = {}
    with open("./data/created/reviews.json") as f:
        reviews_it = json.load(f)
    user_json, hotel_json, date_json = make_init_user_hotels(reviews_it)
    make_user_list(reviews_it, user_json, date_json, hotel_json)
    
if __name__ == '__main__':
    main_creator()


# In[ ]:



