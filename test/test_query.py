#!/usr/bin/env python
# -*- coding: utf-8 -*-
from twcom.query import *


def test_comboss(id):
    # id = getid(u'東賢投資有限公司')
    # print id
    names, coms = zip(*[x.split('\t') for x in getcomboss(id)])
    sprint(names)
    comdic = defaultdict(int)
    ret = cn.bossnode.find({'name': {'$in': names},
                            'coms': {'$in': [id]}})
    for r in ret:
        for id in r['coms']:
            comdic[id] += 1

    df = pd.DataFrame(pd.Series(comdic, name='count'))
    df['name'] = map(getname, df.index)
    df['seat'] = [0]*len(df)

    for r in cn.cominfo.find({'id': {'$in': df.index.tolist()}}):
        df.ix[r['id'], 'seat'] = r['boardcnt']
    df['ratio'] = df.T.apply(lambda x: float(x['count'])/x['seat']*100 if x['seat'] > 0 else 0)
    df = df.sort('ratio')
    ids = df[df['ratio'] > 50].index.tolist()

    for id in ids:
        print id, getname(id)
    return ids


def test_boss_network():
    id = '70827383'
    id = getid(u'中央投資股份有限公司')
    print id
    names = list(getcomboss(id))
    G = get_boss_network(names, maxlvl=1)
    print len(G.node), sum([len(v) for k, v in G.edge.iteritems()])
    fill_boss_info(G)

    dic = defaultdict(int)
    for k, v in G.node.iteritems():
        for id in v['coms']:
            dic[id] += 1
    # dic=pd.Series(dic)
    # dic.sort(ascending=False)
    # dic = pd.DataFrame(dic)
    # dic['name'] = map(getname, dic.index)
    betw = pd.Series(nx.betweenness_centrality(G))
    betw.sort(ascending=True)
    betw.index = [x for x in betw.index]
    print betw


    #betw.tail(20).plot(kind='barh')
    nx.draw_graphviz(G)
    plt.show()

    exp_boss(G, 'test.json')


