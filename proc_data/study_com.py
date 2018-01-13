#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
import numpy as np
import pandas as pd
from twcom.utils import db
from twcom.work import getdf
import seaborn as sns
sns.set_context('poster')
pd.__version__


##
# load id_name
id_name = getdf(db.cominfo1.find({}, {'_id': 0}))


##
id_name = id_name.sort_values(['id', 'name'])
id_name['idx'] = id_name['id'].shift(-1)
iddic = id_name[id_name['id'] != id_name['idx']].drop('idx', axis=1)


##
# get ivst count
coll = db.boards1
ret = coll.aggregate([
    {'$match': {'instid': {'$ne': np.nan}}},
    {'$group': {'_id': {'instid': '$instid', 'id': '$id'}}},
    {'$group': {'_id': '$_id.instid', 'cnt': {'$sum': 1}}},
    {'$sort': {'cnt': -1}},
    {'$limit': 1000}
])
ret = (
    getdf(ret)
    .rename(columns={'_id': 'id'})
    .merge(iddic, how='left')
)


##
(
    ret[['id', 'name', 'source', 'status', 'cnt']]
    .to_csv('doc/ivst_rank.csv', encoding='utf8',
            sep='\t', index=False)
)


##
# Create Company link
coll = db.boards1
ret = coll.aggregate([
    {'$match': {'instid': {'$ne': np.nan}}},
    {'$group': {'_id': {'instid': '$instid', 'id': '$id'},
                'cnt': {'$sum': 1}}},
    {'$sort': {'cnt': -1}},
])
ret = getdf(ret)
ret = (
    ret.join(pd.DataFrame(ret['_id'].to_dict()).T)
    .drop('_id', axis=1)
    .rename(columns={'instid': 'src', 'id': 'dst'})
)

board_cnt = coll.aggregate([
    {'$match': {u'職稱': {'$ne': u'法人代表'},
                'id': {'$in': ret['dst'].tolist()}}},
    {'$group': {'_id': '$id', 'cnt': {'$sum': 1}}},
    {'$sort': {'cnt': -1}},
], allowDiskUse=True)
board_cnt = (
    getdf(board_cnt)
    .rename(columns={'cnt': 'total', '_id': 'dst'}))

ret = ret.merge(board_cnt, how='left')
ret['seat_ratio'] = ret.cnt.astype('float') / ret.total
ret = (
    ret
    .drop('total', axis=1)
    .merge(iddic[['id', 'name']]
           .rename(columns={'id': 'src', 'name': 'src_name'}))
    .merge(iddic[['id', 'name']]
           .rename(columns={'id': 'dst', 'name': 'dst_name'}))
)


##
coll = db.ComLink1
coll.drop()
coll.insert_many(ret.to_dict('record'))
print('%s inserted %s items' % (coll.name, coll.count()))


##
# Study board list
def get_boards(id):
    ret = db.boards.find({'id': id})
    return getdf(ret)


##
