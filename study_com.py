#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
import numpy as np
import pandas as pd
from twcom.utils import db
from twcom.work import getdf
from matplotlib import pylab as plt
import seaborn as sns
sns.set_context('poster')
plt.ion()
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
