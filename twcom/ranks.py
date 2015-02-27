#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils import *
from work import *
from twcom import query
import numpy as np
import itertools as it
from traceback import print_exc
from pdb import set_trace
import re
from collections import defaultdict
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
    ret = cn.comivst.aggregate([
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


def loadall():
    g = nx.DiGraph()
    ret = cn.comivst.find()
    for r in ret:
        g.add_edge(r['src'], r['dst'], size=r['seat'])
    return g


def rankcapital(n=10, cond=None):
    if not cond:
        cond = {}
    ret = cn.cominfo.find(cond).sort('capital', -1).limit(n)
    df = getdf(ret)
    df['rank'] = list(ranking(df['capital']))
    df = df[['rank', 'name', 'id', 'capital']]
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
    ret = cn.boards.aggregate([
        {'$match': {'repr_inst': {'$ne': ''}}},
        {'$group': {'_id': {'name': '$name', 'target': '$target'},
                    'cnt': {'$sum': 1}
                    }},
        {'$sort': SON([('cnt', -1)])},
        {'$limit': n}
        ])
    df = getdf(ret['result'])
    df['rank'] = list(ranking(df['cnt']))
    df['name'] = [x['name'] for x in df['_id']]
    df['target'] = [x['target'] for x in df['_id']]
    df = df[['rank', 'name', 'target', 'cnt']]

    return df


def rankbosscoms(n=10):
    # 董監事代表公司數排名
    ret = cn.bossnode.find().limit(10)
    df = [{'name': r['name'], 'target': r['target'], 'cnt': len(r['coms'])}
          for r in ret]
    df = pd.DataFrame(df)
    df.sort(columns='cnt', ascending=False, inplace=True)
    df['rank'] = list(ranking(df['cnt']))
    df = df[['rank', 'name', 'target', 'cnt']]

    return df


def insdb(coll, name, rankby, df):
    dic = {'data': name, 'rankby': rankby}

    def insparm(args):
        x = args[1].to_dict()
        return x

    dic['ranks'] = map(insparm, df.iterrows())
    insitem(cn, coll, dic)


def insranking():
    coll = 'ranking'
    cn[coll].drop()

    df = rankivst(100000)
    insdb(coll, 'twcom', 'ivst', df)

    df = ranksons(100000)
    insdb(coll, 'twcom', 'sons', df)

    df = rankinst(10000)
    insdb(coll, 'twcom', 'inst', df)

    df = rankbosscoms(10000)
    insdb(coll, 'twcom', 'bosscoms', df)

    cond = {'type': {'$nin': [u'社團', u'財團']}}
    df = rankcapital(100000, cond)
    insdb(coll, 'twcom', 'capital', df)

    cond = {'type': {'$in': [u'社團', u'財團']}}
    df = rankcapital(100000, cond)
    insdb(coll, 'twfund', 'capital', df)

