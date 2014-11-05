#!/usr/bin/env python
# -*- coding: utf-8 -*-
#from output import *
import numpy as np
import pandas as pd
from work import *
import itertools as it
import networkx as nx
from traceback import print_exc
from pdb import set_trace
import re
from collections import defaultdict
from utils import *


def get_boss_network(names, maxlvl=1, level=0, items=None, G=None):
    # 逐級尋找上下游法人股東

    if not hasattr(names, '__iter__'):
        names = [names]

    if G is None:
        G = nx.DiGraph()

    if items is None:
        items = {name: level for name in names}

    ret = cn.bossedge.find({
        '$or': [{'src': {'$in': names}},
                {'dst': {'$in': names}}]})
    newlvl = level + 1
    for r in ret:
        if (newlvl > maxlvl) and \
                not all([x in items for x in (r['src'], r['dst'])]):
            continue
        G.add_edge(r['src'], r['dst'], {'width': r['cnt']})

        if r['src'] not in items:
            items[r['src']] = newlvl
        if r['dst'] not in items:
            items[r['dst']] = newlvl

    if level == 0:
        for lvl in xrange(1, maxlvl+1):
            ids1 = [key1 for key1, lvl1 in it.ifilter(
                    lambda x: lvl == x[1], items.iteritems())]
            get_boss_network(ids1, maxlvl=maxlvl,
                             level=lvl, items=items, G=G)
    return G


def get_network(ids, maxlvl=1, level=0, items=None, G=None):
    # 逐級尋找上下游法人股東

    if not hasattr(ids, '__iter__'):
        ids = [ids]

    if G is None:
        G = nx.DiGraph()
        for r in ids:
            G.add_node(r)

    if items is None:
        items = {id: level for id in ids}

    newlvl = level + 1
    ret = cn.comivst.find({
        '$or': [{'src': {'$in': ids}},
                {'dst': {'$in': ids}}]})

    for r in ret:
        if (newlvl > maxlvl) and \
                not all([x in items for x in (r['src'], r['dst'])]):
            continue
        G.add_edge(r['src'], r['dst'], {'width': r['seat']})

        if r['src'] not in items:
            items[r['src']] = newlvl
        if r['dst'] not in items:
            items[r['dst']] = newlvl

    if level == 0:
        for lvl in xrange(1, maxlvl+1):
            ids1 = [key1 for key1, lvl1 in it.ifilter(
                    lambda x: lvl == x[1], items.iteritems())]
            get_network(ids1, maxlvl=maxlvl,
                        level=lvl, items=items, G=G)

    return G


def getidlike(names):
    # look for match id given name
    if not hasattr(names, '__iter__'):
        names = [names]
    for name in names:
        ret = cn.iddic.find({'name': {'$regex': name}})
        for r in ret:
            if len(r['id']) > 1:
                logger.warning(u'Duplicate id: %s', name)
            yield {'name': r['name'], 'id': r['id'][0]}


def get_network_names(names, maxlvl=1):
    # get network by company's name
    ids = [x['id'] for x in getidlike(names)]
    return get_network(ids, maxlvl=maxlvl)


def bosskey(name=None, comid=None):
    # get boss key
    assert(not (id is None and comid is None))
    return u'\t'.join([name, comid])


def invbosskey(key):
    # inverse boss key
    if key:
        return key.split(u'\n')


def get_boss(id):
    # get boss list
    if not hasattr(id, '__iter__'):
        id = [id]
    for r in cn.boards.find({'id': {'$in': id},
                             'title': {'$not': re.compile(u'.*獨立.*')},
                             'name': {'$ne': u'缺額'}}):
        yield r


def fixname(name):
    # return reduced company name
    return name.replace(u'股份有限公司', u'')


def clustering(xdic, k_clut=10):
    # Clustering label ub k_clut clusters
    xs = sorted(xdic.values())
    N = len(xdic)
    x_uniq = np.unique(xs)
    if len(x_uniq) <= k_clut:
        x_uniq.sort()
        output = {k: round(float(np.argwhere(x_uniq == y).ravel()[0] + 1) /
                  len(x_uniq)*k_clut) for k, y in xdic.iteritems()}
    else:
        li = np.ones(k_clut, dtype=np.float64) * min(xs)
        j, target = 0, 0
        for i, y in enumerate(xs):
            p = float(i) / N
            if p >= target:
                li[j] = y
                j += 1
                target = np.float64(j) / k_clut
        try:
            li = np.unique(li)
            output = {k: round(float(np.argwhere(y >= li)[-1, 0] + 1) /
                      len(li)*k_clut) for k, y in xdic.iteritems()}
        except:
            set_trace()
    return output


def setnode(G, col, dic):
    for k, v in dic.iteritems():
        if k in G.node:
            G.node[k][col] = v


def translate(vdic, dstlim, vlim=None):
    vs = vdic.values()
    if vlim:
        vmin, vmax = vlim
    else:
        vmin = min(vs)
        vmax = max(vs)

    rngd = float(dstlim[1] - dstlim[0])
    rngv = float(vmax - vmin)
    r = rngd / rngv
    return {k: (dstlim[0] + r * float(v - vmin)) for k, v in vdic.iteritems()}


def exp_boss(G, jsonfi):
    if len(G.node) == 0:
        return
    G = G.to_undirected()
    fill_boss_info(G)

    sizedic = {}
    for name, dic in G.node.iteritems():
        try:
            sizedic[name] = len(dic['coms'])
        except:
            print_exc()
            set_trace()

    deg = translate(sizedic, [5, 50])
    setnode(G, 'size', deg)

    ngrp = sum([v.get('group', 0) for v in G.node.values()])
    if ngrp == 0:
        print 'add group'
        output = clustering(nx.betweenness_centrality(G))
        setnode(G, 'group', output)
    exp_graph(G, jsonfi)


def exp_company(G, jsonfi, **kwargs):
    if len(G.node) == 0:
        return
    fill_company_info(G)

    if any([('size' not in dic) for dic in G.node.values()]):
        G1 = G.to_undirected(G)
        try:
            degs = translate(nx.degree_centrality(G1), [8, 60])
        except:
            degs = {k: 8 for k in G.node}
        setnode(G, 'size', degs)

    if any([('group' not in dic) for dic in G.node.values()]):
        output = clustering(nx.betweenness_centrality(G1))
        setnode(G, 'group', output)

    exp_graph(G, jsonfi)


def getBoardbyID(args):
    boards = cn.boards.find({'id': {'$in': args}})
    df = getdf(boards)
    dic = {k: v[
        ['no', 'title', 'name', 'equity', 'repr_instid', 'repr_inst']
        ].sort('no') for k, v in df.groupby('id')}
    return dic


def showkv(id, name, info=None, board=None):
    s1 = []

    if info is None:
        s1.append(u'統一編號: %s' % id)
        s1.append(u'組織名稱: %s' % name)
    else:
        cols = [('id', u'統一編號'), ('name', u'公司名稱'),
                ('status', u'公司狀況'), ('eqtstate', u'股權狀況')]
        for col, colname in cols:
            if col in info:
                s1.append(u'%s: %s' % (colname, unicode(info[col])))

    if board is not None:
        for k, q in board.iterrows():
            s1.append(u' '.join(map(unicode, q)))
    else:
        s1.append(u'無董監事資料')

    return u'\n'.join(s1)
# ===


def fill_company_info(G):
    """ Fill company info,
    remove company whose status not like '核准'.
    """

    ids = G.node.keys()
    dic = {'id': {'$in': ids}, 'title': {'$ne': u'法人代表'}}
    infos = {r['id']: r for r in cn.cominfo.find(dic)}
    boards = {k: v for k, v in getdf(cn.boards.find(dic)).groupby('id')}
    noderm = []
    for id, dic in G.node.iteritems():
        info = infos[id] if id in infos else None
        board = boards[id][[
            'no', 'title', 'name', 'equity', 'repr_instid', 'repr_inst']].sort('no')\
            if id in boards else None
        if info:
            name = info['name']
        else:
            name = id
        dic['name'] = name
        dic['tooltip'] = showkv(id, name, info, board)
        dic['capital'] = info['capital'] if info and 'capital' in info else 0
        if info and 'status' in info:
            dic['status'] = info['status']
            if u'核准' not in info['status']:
                noderm.append(id)
        else:
            dic['status'] = u''
        dic['name'] = fixname(dic['name'])

    G.remove_nodes_from(noderm)
    return G


def fill_boss_node(G, names, coms):
    ret = cn.bossnode.find({
        'name': {'$in': list(names)},
        'coms': {'$in': list(coms)}})
    for r in ret:
        key = bosskey(r['name'], r['target'])
        if key in G.node:
            node = G.node[key]
            node['coms'] = r['coms']
            node['name'] = r['name']


def fill_boss_info(G):
    names, coms = zip(*[x.split(u'\t') for x in G.node.keys()])
    names, coms = set(names), set(coms)

    fill_boss_node(G, names, coms)
    [coms.update(x['coms']) for x in G.node.values()]
    ret = cn.boards.find({
        'name': {'$in': list(names)},
        'id': {'$in': list(coms)}})
    dic = defaultdict(list)
    for r in ret:
        key = bosskey(r['name'], r['target'])
        dic[key].append((r['id'], r['title']))
    for k, v in dic.iteritems():
        if k not in G.node:
            continue
        node = G.node[k]
        v = sorted(v)

        grpli = [k.split(u'\t')[0]]
        for id, grp in it.groupby(v, lambda x: x[0]):
            com = getname(id)
            grp = list(grp)
            if len(grp) > 1:
                grpli.append(u'\n'.join(
                    [u'\t'.join([com, x[1]]) for x in grp
                        if x[1] != u'法人代表']
                ))
            else:
                grpli.append(u'\t'.join([com, grp[0][1]]))
        node['titles'] = grpli
        node['tooltip'] = u'\n'.join(grpli)
        node['size'] = len(node['coms'])

    for k, v in G.node.iteritems():
        assert('titles' in v)
    return G


def getbosskey(name, id):
    ret = cn.bossnode.find_one(
        {'name': name,
         'coms': {'$in': [id]}})
    if ret:
        return u'\t'.join((ret['name'], ret['target']))
    else:
        return None


def getcomboss(id):
    if not hasattr(id, '__iter__'):
        id = [id]

    ret = cn.boards.find({'id': {'$in': id}})
    for r in ret:
        yield getbosskey(r['name'], r['target'])


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
    df['ratio'] = df.T.apply(lambda x: float(x['count'])/x['seat']*100 if x['seat']>0 else 0)
    df = df.sort('ratio')
    ids = df[df['ratio']>50].index.tolist()
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


def get_bossesnet(ids):
    names = list(getcomboss(ids))
    G = get_boss_network(names, maxlvl=1)
    fill_boss_info(G)
    return G


from matplotlib import pylab as plt
if __name__ == '__main__':
    """"""
    # test()
    # execfile('makeindex.py')
    # G = get_boss_network(u'王文淵')
    # print len(G.node)
    # exp_boss(G, 'test.json')
    # nx.draw_graphviz(G)
    # plt.show()

    # for r in get_name_like(u'統一'):
    #     print r
    #test_boss_network()
    #get_bossesnet(['04278323', '28428379'])

    #test_comboss()
