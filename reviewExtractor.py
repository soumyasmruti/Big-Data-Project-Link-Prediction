import glob, json
import dateutil.parser
from xml.etree import ElementTree
        
def write_json(d, fname):
    """Writes dictionary d to fname"""
    with open(fname, 'w') as f:
        f.write(json.dumps(d))

def get_date(date):
    return dateutil.parser.parse(date)


def agregateRating(review):
    newrating = {}
    keys = review.keys()
    if len(keys) == 1:
        newrating["Business service"] = review["Overall"]
        newrating["Internet access"] = review["Overall"]
        newrating["front desk"] = review["Overall"]
        newrating["Cleanliness"] = review["Overall"]
        newrating["Location"] = review["Overall"]
        newrating["Overall"] = review["Overall"]
        newrating["Rooms"] = review["Overall"]
        newrating["Service"] = review["Overall"]
        newrating["Sleep Quality"] = review["Overall"]
        newrating["Value"] = review["Overall"]
    else:
        newrating["Business service"] = review.get("Business service", review["Overall"])
        newrating["Internet access"] = review.get("Internet access", review["Overall"])
        newrating["front desk"] = review.get("front desk", review["Overall"])
        newrating["Cleanliness"] = review.get("Cleanliness", review["Overall"])
        newrating["Location"] = review.get("Location", review["Overall"])
        newrating["Overall"] = review["Overall"]
        newrating["Rooms"] = review.get("Rooms", review["Overall"])
        newrating["Service"] = review.get("Service", review["Overall"])
        newrating["Sleep Quality"] = review.get("Sleep Quality", review["Overall"])
        newrating["Value"] = review.get("Value", review["Overall"])
    return newrating

def extractHotelAddress(xmlstr):
    try:
        tree = ElementTree.ElementTree(ElementTree.fromstring(xmlstr))
        root = tree.getroot()
        for neighbor in root.iter('span'):
            if neighbor.get("property") == "v:locality":
                return neighbor.text
    except:
        return ""
   
def loadInputFile():
    files = glob.glob("./json/*.json")
    newdata = {}
    for each in files:
        with open(each) as f:
            data = json.load(f)
            for review in data['Reviews']:
                newRating = agregateRating(review['Ratings'])
                newdata[review['ReviewID']] = {'Ratings': newRating, 
                                               'Author': review['Author'],
                                               'Author Location': review.get("'AuthorLocation'", "").split(",")[0],
                                               'Date': str(get_date(review['Date'])), 
                                               'HotelID': data['HotelInfo']["HotelID"],
                                               'HotelLocation': extractHotelAddress(data['HotelInfo'].get("Address", "")),
                                              }
            
    write_json(newdata, "./data/created/reviews.json")

loadInputFile()