#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
import bson
import networkx as nx
import pandas as pd
from twcom.utils import db
from twcom.work import show, getdf
show(pd.__version__)


##
# Check distribution of series
def chkdist(s):
    name = s.name
    ret1 = (
        s
        .value_counts()
        .rename('cnt')
        .reset_index()
        .rename(columns={
            'index': name,
        })
    )
    ret1['len'] = ret1[name].apply(len)
    return ret1


##
def upd_link(df0, links):
    df1 = (
        df0[df0['id'].isin(links['dst'])]
        .rename(columns={'id': 'dst'})
        .merge(links)
        .merge(df0[df0['id'].isin(links['src'])]
               .rename(columns={'id': 'src'}),
               on=['src', u'姓名'],
               suffixes=['', '_1'])
    )
    df2 = (
        df1
        .groupby(u'姓名')
        .apply(lambda x: nx.Graph(x[['src', 'dst']].values.tolist()))
    )

    df3 = []
    for name, G in df2.iteritems():
        for i, x in enumerate(nx.connected_components(G)):
            df3.extend([(name, x1, i) for x1 in x])
    df3 = pd.DataFrame(df3, columns=(u'姓名', 'id', 'no')).merge(df0)
    df3 = (
        df3
        .merge(
            df3.groupby([u'姓名', 'no'])['ref'].min()
            .rename('reffix')
            .reset_index()
        )
    )
    df3 = df3[df3.ref != df3.reffix]

    df4 = df0.merge(df3, how='left', indicator=True)
    df4.loc[df4._merge == 'both', 'ref'] = \
        df4.loc[df4._merge == 'both', 'reffix']
    df4 = (
        df4.drop('no', axis=1)
        .drop('_merge', axis=1)
        .drop('reffix', axis=1)
    )
    assert len(df0) == len(df4)
    return df4


##
boards = getdf(db.boards1.find({u'姓名': {'$ne': u''}}))
boards['ref'] = boards['_id'].apply(str)
df0 = (boards[['id', u'姓名', 'ref']])


##
# link by company link
links = getdf(db.comLink1.find({}, {'src': 1, 'dst': 1, '_id': 0}))
df0 = upd_link(df0, links)
print(df0.shape)


##
# link by same name group
cnt = df0['ref'].drop_duplicates().shape
while 1:
    df1 = df0.merge(df0, on=[u'姓名'])
    df1 = df1[df1['id_x'] != df1['id_y']]
    df1['id_1'] = df1[['id_x', 'id_y']].min(axis=1)
    df1['id_2'] = df1[['id_x', 'id_y']].max(axis=1)
    df1 = df1[['id_1', 'id_2', u'姓名']].drop_duplicates()
    df2 = df1.groupby(['id_1', 'id_2'])[u'姓名'].count().rename('cnt')
    df2 = df2[df2 > 1]
    df2 = (
        df2.reset_index()
        .rename(columns={'id_1': 'src', 'id_2': 'dst'})
        .drop('cnt', axis=1)
    )

    df0 = upd_link(df0, df2)
    cnt1 = df0['ref'].drop_duplicates().shape
    print(cnt, cnt1)
    if cnt == cnt1:
        break
    cnt = cnt1


##
# Merge back to boards
df0['ref'] = df0['ref'].apply(bson.ObjectId)
boards = boards.drop('ref', axis=1).merge(df0)

##
coll = db.boards1
coll.drop()
coll.insert_many(
    boards
    .drop('_id', axis=1)
    .to_dict('record')
)
print('%s inserted %s items' % (coll.name, coll.count()))

##
