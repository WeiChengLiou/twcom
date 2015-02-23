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
    df['name'] = getnamedf(df['_id'].values.tolist())
    df = df[['rank', 'name', '_id', 'cnt']]

    return df


def loadall():
    g = nx.DiGraph()
    ret = cn.comivst.find()
    for r in ret:
        g.add_edge(r['src'], r['dst'], size=r['seat'])
    return g


def ranksons(n=10):
    g = loadall()
    dic = {}
    for k in g.node:
        dic[k] = len(nx.descendants(g, k))
    df = pd.Series(dic)
    df.sort(ascending=False)
    df = pd.DataFrame(df, columns=['cnt'])
    df['_id'] = df.index

    df['rank'] = list(ranking(df['cnt']))
    df = df[df['rank'] <= n]
    df['name'] = getnamedf(df.index.tolist())
    df.index = range(len(df))
    df = df[['rank', 'name', '_id', 'cnt']]

    return df

