#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools as it
from collections import defaultdict
import pandas as pd
from twcom.utils import db, insitem, chk_board
from twcom.work import getdf
from twcom import query
from bson.son import SON
import networkx as nx


def getnamedf(ids):
    namedic = query.getnamedic(ids)
    return [namedic[k] for k in ids]


def ranking(s):
    x0, r = s[0], 0
    for i, x in enumerate(s):
        if x != x0:
            r = i
            x0 = x
        yield r + 1


def rankivst(n=10):
    # 直接投資排名
    # Return rankings of direct investment (n_ivst)
    # Cols: Rank, Name, id, n_ivst
    """"""
    ret = db.ComBosslink.aggregate([
        {'$match': {'ivst': 1}},
        {'$group': {'_id': '$src', 'cnt': {'$sum': 1}}},
        {'$sort': SON([('cnt', -1)])},
        ])
    df = getdf(ret['result'])
    df['rank'] = list(ranking(df['cnt']))
    df = df[df['rank'] <= n]
    df.rename(columns={'_id': 'id'}, inplace=True)
    df['name'] = getnamedf(df['id'].values.tolist())
    df = df[['rank', 'name', 'id', 'cnt']]

    return df


def rankCentrality(G):
    df = pd.DataFrame.from_dict({
        'closeness': nx.closeness_centrality(G),
        'betweeness': nx.betweenness_centrality(G),
        'degree': nx.degree_centrality(G)
        })

    df['rankclose'] = df['closeness'].rank()
    df['rankbetween'] = df['betweeness'].rank()
    df['rankDegree'] = df['degree'].rank()

    # insdb(coll, data, 'CentralMax', df)
    return df


def loadall():
    g = nx.DiGraph()
    ret = db.ComBosslink.find()
    for r in ret:
        g.add_edge(r['src'], r['dst'], size=r['bossCnt'])
    return g


def rankcapital(n=10, cond=None):
    if not cond:
        cond = {}
    ret = db.cominfo.find(cond, {'_id': 0, 'name': 1, 'id': 1, 'capital': 1}).\
        sort('capital', -1).limit(n)
    df = list(ret)
    capitals = [x['capital'] for x in df]
    [x.__setitem__('rank', c) for x, c in it.izip(df, ranking(capitals))]
    return df


def ranksons(n=10):
    # 子孫數排名
    g = loadall()
    dic = {}
    for k in g.node:
        dic[k] = len(nx.descendants(g, k))
    df = pd.Series(dic)
    df.sort(ascending=False)
    df = pd.DataFrame(df, columns=['cnt'])
    df['id'] = df.index

    df['rank'] = list(ranking(df['cnt']))
    df = df[df['rank'] <= n]
    df['name'] = getnamedf(df.index.tolist())
    df.index = range(len(df))
    df = df[['rank', 'name', 'id', 'cnt']]

    return df


def rankinst(n=10):
    # 法人代表排名

    df = defaultdict(int)
    ret = db.cominfo.find()
    for r in ret:
        for b in r['boards']:
            if (b['repr_inst'] != "") and (chk_board(b['name'])):
                key = b['name'], b['target']
                df[key] += 1
    df = pd.Series(df, name='cnt')
    df.sort_values(ascending=False, inplace=True)
    df = df.head(n)

    df = pd.DataFrame(df)
    df['name'] = [x[0] for x in df.index]
    df.index = range(len(df))
    df['rank'] = list(ranking(df['cnt']))
    # df['target'] = [x['target'] for x in df['_id']]

    return df


def rankbosscoms(n=10):
    # 董監事代表公司數排名
    ret = db.bossnode.find()
    df = [{'name': r['name'], '_id': str(r['_id']), 'cnt': len(r['orgs'])}
          for r in ret if chk_board(r['name'])]
    df = pd.DataFrame(df)
    df.sort_values(columns='cnt', ascending=False, inplace=True)
    df = df.head(n)
    df.index = range(len(df))
    df['rank'] = list(ranking(df['cnt']))

    return df


def insdb(coll, name, rankby, df):
    dic = {'data': name, 'rankby': rankby}

    def insparm(args):
        x = args[1].to_dict()
        return x

    if isinstance(df, pd.DataFrame):
        dic['ranks'] = map(insparm, df.iterrows())
    else:
        dic['ranks'] = df
    insitem(db, coll, dic)


def inscomrank():
    coll = 'ranking'
    # db[coll].drop()

    # df = rankivst(10000)
    # insdb(coll, 'twcom', 'ivst', df)

    # df = ranksons(10000)
    # insdb(coll, 'twcom', 'sons', df)

    db[coll].remove({'data': 'twcom', 'rankby': 'inst'})
    df = rankinst(10000)
    insdb(coll, 'twcom', 'inst', df)

    db[coll].remove({'data': 'twcom', 'rankby': 'bosscoms'})
    df = rankbosscoms(10000)
    insdb(coll, 'twcom', 'bosscoms', df)

    # df = rankcapital(10000, comcond)
    # insdb(coll, 'twcom', 'capital', df)


def insCentralRank(df):
    dicAll = {'data': 'twcom', 'rankby': 'Central'}
    li = []
    for id, dr in df.iterrows():
        dic = {'id': id}
        dic.update(dr.to_dict())
        li.append(dic)
    dicAll['ranks'] = li
    db['ranking'].insert(dicAll)


def updCentralInfo(df):
    dic = df.T.to_dict()
    ret = db.cominfo.find()
    for r in ret:
        d = dic.get(r['id'])
        if d:
            r['central'] = d
            db.cominfo.save(r)


def insfundrank(coll):
    cond = {'type': {'$nin': [u'社團', u'財團']}}
    df = rankcapital(10000, cond)
    insdb(coll, 'twcom', 'capital', df)

    cond = {'type': {'$in': [u'社團', u'財團']}}
    df = rankcapital(10000, cond)
    insdb(coll, 'twfund', 'capital', df)
