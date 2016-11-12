#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
import re
import pandas as pd
from twcom.utils import db
from twcom.work import show, replaces, yload
pd.__version__


##
# Retrieve bad name list


##
ret = db.raw.find_one(
    {'$and': [
        {u'公司名稱': {'$exists': 0}},
        {u'分公司名稱': {'$exists': 0}},
        {u'商業名稱': {'$exists': 0}},
    ]},
    {'_id': 0})
show(ret)


##
# Construct (id, name) pair dictionary
bads = yload('doc/badstatus.yaml')
namecol = [u'公司名稱']
ret = db.raw.find(
    {},
    {'_id': 0, 'id': 1,
     u'公司名稱': 1,
     u'公司狀況': 1,
     u'公司狀況文號': 1,
     }
)
id_name = []


skips = [
    u'（同名）',
    u'（無統蝙）',
    u'（無統編）',
    u'（股份有限公司）',
    u'（因偽造文書撤銷公司設立登記）',
    u'（父子）',
    u'（新增統編）',
    u'(在臺灣地區公司名稱)',
    u'(在大陸地區公司名稱)',
    u'株式會社',
    u'■■■■■',
    u'有限會社',
]


def fun(key, x):
    x1 = replaces(x.strip(), skips)
    if x1:
        id_name.append(key + (x1,))


for r in ret:
    for col in namecol:
        name = r.get(col)
        if name:
            break
    if not name:
        continue

    word = r.get(u'公司狀況文號', None)
    status = r[u'公司狀況']
    id = r['id']
    key = (id, status, word)
    if isinstance(name, list):
        for x in name:
            if isinstance(x, basestring):
                fun(key, x)
            else:
                for x1 in x:
                    fun(key, x1)
    elif isinstance(name, basestring):
        fun(key, name)


id_name = (
    pd.DataFrame(id_name, columns=['id', 'status', 'word', 'name'])
)


df1 = id_name[~id_name.status.isin(bads)]
s = df1.name.value_counts().sort_values()
dbls = s[s > 1].index.get_level_values(0)
dbl_id = (
    id_name
    [id_name['name'].isin(dbls)]
    ['name']
    .drop_duplicates()
    .tolist()
)


##
# Study bad company name
def ing_badname():
    li = []
    rx = re.compile(u'([\(（].*?[\)）])', re.UNICODE)

    for i, r in id_name.iterrows():
        for x in rx.findall(r['name']):
            li.append((r['id'], r['status'], r['word'], x))

    li = pd.DataFrame(li, columns=['id', 'status', 'word', 'name'])

    li1 = li['name'].value_counts().sort_values()
    li1.to_csv('temp.csv', sep='\t', encoding='utf8')
    return li


##
# fix bad board name


##
# fix bad inst id/name


##
