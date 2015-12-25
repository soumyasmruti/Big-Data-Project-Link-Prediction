
# coding: utf-8

# In[21]:

import pandas as pd
import snap, datetime, math, json
from collections import defaultdict
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import roc_auc_score, roc_curve
from operator import itemgetter


def load_json(fname):
    with open(fname) as f:
        return json.load(f)
    
def write_json(d, fname):
    with open(fname, 'w') as f:
        f.write(json.dumps(d))
    
def score_ext(tot, f):
    return pd.read_json('./data/' + tot + '/' + f + '.json')

def feature_extractor(i, j, scores, users, hotels):
    features = {sn: scores[sn][i][j] for sn in scores}
    features["hotel_review"] = float(hotels["ReviewCount"])
    features["user_review"] = float(users["ReviewCount"])
    features["hotel_loc"] = float(hotels["HotelLocation"])
    features["user_loc"] = float(users["UserLocation"])

    return features
    
def build_train_test(data, vectorizer):
    matrix = load_json('./data/' + data + '/matrix.json')
    users = load_json('./data/' + data + '/user.json')
    hotels = load_json('./data/' + data + '/hotels.json')
    
    scores = {f: load_json('./data/' + data + '/' + f + '.json') for f in
                            ['svd', 'random_walks_by_weight', 'random_walks','hotel_cn',
                            'hotel_jac', 'user_cn', 'user_jac']}
    
    feature_dataframe, y, z = pd.DataFrame(), pd.DataFrame(), []
    for i in matrix:
        for j in matrix[i]:
            z.append((i, j))
            y.append(matrix[i][j])
            feature_dataframe.append(feature_extractor(i, j, scores, users[i], hotels[i]))
            feature_dataframe.fillna(0)
    
    frame_as_dic = [dict(r.iteritems()) for _, r in feature_dataframe.iterrows()]
    if data == "train":
        X = vectorizer.fit_transform(frame_as_dic) 
    else:
        X = vectorizer.transform(frame_as_dic)

    return X, y, z

def classifier():
    vectorizer = DictVectorizer(sparse=False)
    X_train, y_train, z_train = build_train_test("train", vectorizer)
    X_test, y_test, z_test = build_train_test("test", vectorizer)
    clf = GradientBoostingClassifier(n_estimators = 5000, max_depth = 5)
    clf2 = RandomForestClassifier(n_estimators = 2000, max_depth = 4)
    clf.fit(X_train, y_train)
    clf2.fit(X_train, y_train)
    prediction = clf.predict_proba(X_test)[:, 1]
    prediction2 = clf2.predict_proba(X_test)[:, 1]
    scores = defaultdict(dict)
    for (i, j), p in zip(z_test, prediction):
        scores[i][j] = p
    write_json(scores, './data/test/boosting_classifier.json')
    
    scores2 = defaultdict(dict)
    for (i, j), p in zip(z_test, prediction2):
        scores2[i][j] = p
    write_json(scores2, './data/test/random_classifier.json')

feat_list = ['matrix', 'svd', 'random_walks_by_weight', 'random_walks','hotel_cn', 'hotel_jac', 'user_cn', 'user_jac',
                    'boosting_classifier', 'random_classifier']
    
def evaluation(matrix, feat, prec = 10):
    for i, feature in enumerate(feat):
        predictions = load_json('./data/test/' + feature + '.json')
        tot_pre = 0
        ys, ps = [], []
        for j in predictions:
            y, p = zip(*[(matrix[j][k], predictions[j][k]) for k in predictions[j]])
            n = min(prec, len(y))
            tot_pre += sum(zip(*sorted(zip(y, p), key=itemgetter(1), reverse=True))[0][:n]) / float(n)
            ys += y
            ps += p

        roc_auc = roc_auc_score(ys, ps)
        fpr, tpr, t = roc_curve(ys, ps)
        print "Method:", feature
#         print "  Precision @{:} = {:.4f}".format(prec, tot_pre / len(matrix))
        print "  ROC Auc = {:.4f}".format(roc_auc)
    
if __name__ == '__main__':
    classifier()
    evaluation(load_json('data/test/matrix.json'), feat_list)


# In[ ]:



