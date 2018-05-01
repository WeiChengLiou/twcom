#!/usr/bin/env python
# -*- coding: utf-8 -*-
from twcom.query import getcomboss
from twcom.work import show
from twcom.utils import getid
from vis import output as opt
from collections import defaultdict
from utils import db, getname
import pandas as pd


def test_comboss(self):
    id = getid(u'東賢投資有限公司')
    print(id)
    names, coms = zip(*[x.split('\t') for x in getcomboss(id)])
    show(names)
    comdic = defaultdict(int)
    ret = db.bossnode.find({'name': {'$in': names},
                            'coms': {'$in': [id]}})
    for r in ret:
        for id in r['coms']:
            comdic[id] += 1

    df = pd.DataFrame(pd.Series(comdic, name='count'))
    df['name'] = map(getname, df.index)
    df['seat'] = [0]*len(df)

    for r in db.cominfo.find({'id': {'$in': df.index.tolist()}}):
        df.ix[r['id'], 'seat'] = r['boardcnt']
    df['ratio'] = df.T.apply(lambda x: float(x['count'])/x['seat']*100 if x['seat'] > 0 else 0)
    df = df.sort('ratio')
    ids = df[df['ratio'] > 50].index.tolist()

    for id in ids:
        print(id, getname(id))
    return ids


def test_boss_network():
    id = getid(u'中央投資股份有限公司')
    print(id)
    names = list(getcomboss(id))
    G = get_boss_network(names, maxlvl=1)
    print(len(G.node), sum([len(v) for k, v in G.edge.iteritems()]))

    fi = 'test_bossnet'
    path = 'test'
    exp_boss(G, fi=fi, path=path)
    opt.write_d3(fi, path=path)
    print('export boss network')


def test_com_network():
    id = getid(u'中央投資股份有限公司')
    print(id)
    G = get_network(id, maxlvl=1)
    print(len(G.node), sum([len(v) for k, v in G.edge.iteritems()]))

    fi = 'test_comnet'
    path = 'test'
    exp_company(G, fi=fi, path=path)
    opt.write_d3(fi, path=path)
    print('export company network')
