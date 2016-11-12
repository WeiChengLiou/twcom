#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bson.objectid import ObjectId
from utils import db, init, logger, getnamedic, chk_board
from work import show, getitem, deprecated, getdf, fixname
from work import flatten
import numpy as np
import itertools as it
import networkx as nx
from traceback import print_exc
from pdb import set_trace
import re
from collections import defaultdict
from vis import output as opt
import yaml
CONFIG = yaml.load(open('config.yaml'))


# Basic{{{
def get_boss_network(**kwargs):
    # 逐級建立董監事網絡圖
    if kwargs.get('names'):
        targets = getbosslike(kwargs.get('name', u''))
    else:
        targets = kwargs.get('target', [])
        if not hasattr(targets, '__iter__'):
            targets = [targets]
        if isinstance(targets[0], basestring):
            targets = map(ObjectId, targets)

    def list_boss(boards):
        try:
            return list(it.ifilter(lambda r: r.get('target'), boards))
        except:
            show(boards)
            print_exc()

            raise Exception('boards error')

    G = nx.Graph()
    comdic = {}
    maxlvl = kwargs.get('maxlvl', 1)
    namedic = {str(k): 0 for k in targets}
    db = kwargs.get('db', init(CONFIG['db']))
    method = kwargs.get('method', 0)

    ret = db.bossnode.find({
        '_id': {'$in': targets}})
    for r in ret:
        [comdic.setdefault(x, 0) for x in r['orgs']]
    coms = list(comdic.keys())
    titledic = defaultdict(list)

    def subgraph(G, coms, lvl):
        # 1. 給定 name 與 target，查詢相關公司
        newlvl = lvl + 1
        if not coms:
            return G, coms, newlvl

        # 2. 給定相關公司後，找出同間公司的董監名單
        ret = db.cominfo.find(
            {'id': {'$in': coms},
             'source': 'twcom',
             }, {'id': 1, 'boards': 1, '_id': 0, 'name': 1})
        for rs in ret:
            boards = list_boss(rs['boards'])
            [b.__setitem__('target', str(b['target'])) for b in boards]
            bosset = getitem(boards, 'target')
            addbossedge(G, bosset, method)
            [namedic.setdefault(b, lvl) for b in bosset]
            for b in boards:
                titledic[b['target']].append(
                    u'{0} {1}'.format(rs['name'], b['title']))

        for key0, lvl0 in namedic.iteritems():
            if lvl0 == lvl - 1:
                node = G.node[key0]
                node['titles'] = u'\n'.join(titledic.pop(key0))

        if lvl >= maxlvl:
            return G, coms, newlvl

        newtgt = [ObjectId(key1) for key1, lvl1 in
                  namedic.iteritems() if lvl1 == lvl]
        ret = db.bossnode.find({'_id': {'$in': newtgt}})
        for r in ret:
            r['size'] = len(r['orgs'])
            [comdic.setdefault(x, newlvl) for x in r['orgs']]
            G.node[str(r.pop('_id'))].update(r)

        coms = [k for k, v in comdic.iteritems() if v == lvl]

        return G, coms, newlvl

    G, coms, newlvl = reduce(
        lambda x, y: subgraph(*x),
        xrange(maxlvl + 1),
        (G, coms, 0))
    return G


def addbossedge(G, rs, method=0):
    # 3. 根據不同 graph 需求，畫出同公司董監事間的 boss edge
    if not rs:
        return

    def addedge(key1, key2):
        if G.has_edge(key1, key2):
            dic = G.get_edge_data(key1, key2)
            dic['weight'] += 1
        else:
            G.add_edge(key1, key2, {'weight': 1})

    if method == 0:
        r0 = rs[0]
        for r in rs[1:]:
            addedge(r0, r)
    else:
        for r1, r2 in it.combinations(rs, 2):
            addedge(r1, r2)


def get_network(ids, maxlvl=1, **kwargs):
    # 逐級建立公司網絡圖

    if not hasattr(ids, '__iter__'):
        ids = [ids]

    lnunit = kwargs.get('lnunit', 'ivstCnt')
    cond = {}
    cond['death'] = 0
    cond['ivst'] = kwargs.get('ivst', {'$gt': -1})

    G = nx.DiGraph()
    map(G.add_node, ids)

    items = {id: 0 for id in ids}

    def subgraph(G, ids, lvl):
        newlvl = lvl + 1
        if not ids:
            return G, [], newlvl

        cond['$or'] = [{'src': {'$in': ids}},
                       {'dst': {'$in': ids}}]
        ret = db.ComBosslink.find(cond, {'_id': 0})

        for r in it.ifilter(lambda r: not G.has_edge(r['src'], r['dst']),
                            ret):
            if r['src'] not in items:
                items[r['src']] = newlvl
            if r['dst'] not in items:
                items[r['dst']] = newlvl

            r['width'] = r.get(lnunit, 1)
            G.add_edge(r.pop('src'), r.pop('dst'), r)

        ids1 = [key1 for key1, lvl1 in it.ifilter(
                lambda x: lvl == x[1], items.iteritems())]
        return G, ids1, newlvl

    G, ids, lvl = reduce(lambda x, y: subgraph(*x),
                         xrange(maxlvl), (G, ids, 0))
    return G


# }}}
# Support {{{


def getbosslike(names):
    # look for match boss given boss name
    if not hasattr(names, '__iter__'):
        names = [names]
    for name in names:
        ret = db.bossnode.find({'name': name})
        for r in ret:
            yield r['_id']


def getidlike(names):
    # look for match id given company name
    if not hasattr(names, '__iter__'):
        names = [names]
    for name in names:
        ret = db.iddic.find({'name': {'$regex': name}})
        for r in ret:
            if len(r['id']) > 1:
                logger.warning(u'Duplicate id: %s', name)
            yield {'name': r['name'], 'id': r['id'][0]}


def bosskey(name=None, comid=None):
    # get boss key
    assert(not (id is None and comid is None))
    return u'\t'.join([name, comid])


def invbosskey(key):
    # inverse boss key
    if key:
        return key.split(u'\t')


def get_boss(id, ind=False):
    # get boss list by company id
    # if not hasattr(id, '__iter__'):
    #     id = [id]
    bconds = [(lambda b: chk_board(b['name']))]
    if ind:
        bconds.append((lambda b: u'獨立' not in b['title']))

    ret = db.cominfo.find({'id': id}, {'_id': 0, 'name': 1, 'boards': 1})
    for r in ret:
        r['boards'] = list(it.ifilter(
            lambda b: all(map(lambda f: f(b), bconds)),
            r['boards']))
        [r1.__setitem__('target', str(r1['target'])) for r1 in r['boards']]
        return r


@deprecated
def getBoardbyID(ids):
    # get board list by company ids
    if not hasattr(ids, '__iter__'):
        ids = [ids]
    boards = db.boards.find({'id': {'$in': ids}})
    df = getdf(boards)
    dic = {k: v[
        ['no', 'title', 'name', 'equity', 'repr_instid', 'repr_inst']
        ].sort('no') for k, v in df.groupby('id')}
    return dic


def getbosskey(name, id):
    # get boss key
    ret = db.bossnode.find_one(
        {'name': name,
         'orgs': {'$in': [id]}})
    if ret:
        return u'\t'.join((ret['name'], ret['target']))
    else:
        return None


def getcomboss(ids):
    # get bossnode by company id
    if not hasattr(ids, '__iter__'):
        ids = [ids]

    ret = db.cominfo.find({'id': {'$in': ids}})
    for r in ret:
        for boss in it.ifilter(lambda boss: boss.get('target'),
                               r['boards']):
            yield boss['target']


def fillgrp(g, grps):
    for k, v in g.node.iteritems():
        v['group'] = 0
        for i, grp in enumerate(grps):
            if k in grp:
                v['group'] = i + 1
                break

# }}}
# for Export {{{


def cluster(xdic, k_clut=10):
    # Clustering label in k_clut clusters
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
    # set each col's value with dict
    for k, v in dic.iteritems():
        if k in G.node:
            G.node[k][col] = v


def setedge_width(G, fun):
    for x in G.edges():
        y = G.get_edge_data(*x)
        if y.get('width'):
            # print y['width'], fun(y['width'])
            y['width'] = fun(y['width'])


def translate(vdic, dstlim, vlim=None):
    # translate vdic's value in dstlim scale
    vs = vdic.values()
    if vlim:
        vmin, vmax = vlim
    else:
        vmin = min(vs)
        vmax = max(vs)

    rngd = float(dstlim[1] - dstlim[0])
    rngv = float(vmax - vmin)
    r = rngd / rngv if rngv > 0 else 0.
    return {k: (dstlim[0] + r * float(v - vmin)) for k, v in vdic.iteritems()}


def exp_boss(G, **kwargs):
    # Export boss network
    # Fill info, translate com count into size
    # Cluster betweenness centrality into group
    # Export graph
    if len(G.node) == 0:
        return
    fill_boss_info(G)

    sizedic = {}
    for name, dic in G.node.iteritems():
        sizedic[name] = dic['size']

    deg = translate(sizedic, [5, 50])
    setnode(G, 'size', deg)

    # ngrp = sum([v.get('group', 0) for v in G.node.values()])
    # if ngrp == 0:
    #     output = cluster(nx.betweenness_centrality(G))
    #     setnode(G, 'group', output)
    [v.setdefault('group', 0) for k, v in G.node.iteritems()]

    return opt.exp_graph(G, **kwargs)


# @timeit
def exp_company(G, **kwargs):
    # Export company network
    # Fill info, translate degree centrality into size
    # Cluster betweenness centrality into group
    # Export graph
    if len(G.node) == 0:
        return

    fill_company_info(G)
    G1 = G.to_undirected(G)

    if any([('size' not in dic) for dic in G.node.values()]):
        try:
            degs = translate(nx.degree_centrality(G1), [8, 60])
        except:
            degs = {k: 8 for k in G.node}
        setnode(G, 'size', degs)

    # if any([('group' not in dic) for dic in G.node.values()]):
    #     output = cluster(nx.betweenness_centrality(G1))
    #     setnode(G, 'group', output)
    [v.setdefault('group', 0) for k, v in G.node.iteritems()]

    # print keargs.get('lineunit')
    # if kwargs.get('lineunit') == 'seatratio':
    #     setedge_width(G, lambda x: float(x)/10.)

    return opt.exp_graph(G, **kwargs)


def showkv(id, name, info=None):
    # Prepare cominfo for tooltip
    s1 = []
    boardcol = ('title', 'name', 'repr_inst')

    if info is None:
        s1.append(u'統一編號: %s' % id)
        s1.append(u'組織名稱: %s' % name)
    else:
        coldic = [('id', u'統一編號'), ('name', u'公司名稱'),
                  ('status', u'公司狀況'), ('eqtstate', u'股權狀況')]
        for col, colname in it.ifilter(lambda col: col in info,
                                       coldic):
            s1.append(u'%s: %s' % (colname, unicode(info[col])))

    # Because board maybe None, or DataFrame,
    # but can also be empty DataFrame or not.
    # So combined two conditions here
    if info['boards']:
        for b in info['boards']:
            s1.append(
                u' '.join(map(lambda c: unicode(b[c]), boardcol)))
    else:
        s1.append(u'無董監事資料')

    return u'\n'.join(s1)


def fill_company_info(G):
    # Fill company info,
    # Remove company whose status not like '核准'.

    assert(len(G.node) > 0)
    ids = G.node.keys()
    dic = {'id': {'$in': ids}, 'title': {'$ne': u'法人代表'}}
    infos = {r['id']: r for r in db.cominfo.find(dic)}
    noderm = []
    for id, dic in G.node.iteritems():
        info = infos.get(id)
        if info:
            name = info['name']
        else:
            name = id
        dic['tooltip'] = showkv(id, name, info)
        dic['capital'] = info['capital'] if info and 'capital' in info else 0
        if info and 'status' in info:
            dic['status'] = info['status']
            if u'核准' not in info['status']:
                noderm.append(id)
        else:
            dic['status'] = u''
        dic['name'] = fixname(name)

    G.remove_nodes_from(noderm)
    return G


def fill_boss_node(G, targets):
    # Fill boss node
    ret = db.bossnode.find({'_id': {'$in': targets}})
    for r in ret:
        node = G.node[r['_id']]
        node['orgs'] = r['orgs']
        node['name'] = r['name']


def getli(k, x):
    if 'orgs' not in x:
        print 'Err:', k, x
        raise Exception()
    return x['orgs']


def fill_boss_info(G):
    # Fill boss info

    for k, node in G.node.iteritems():
        node['tooltip'] = u'\n'.join([node['name'], node['titles']])

    for k, v in G.node.iteritems():
        assert('titles' in v)
    return G


# }}}
# {{{Advance search
w2 = u'\u202c'


def queryboss(name):
    name = name.replace(w2, u'')
    ret = list(db.bossnode.find({'name': re.compile(name)}, {'target': 0}))
    ids = set(flatten([r['orgs'] for r in ret]))
    dic = getnamedic(tuple(ids))
    for r in ret:
        r['orgs'] = map(lambda x: dic.get(x, x), r['orgs'])
        r['_id'] = str(r['_id'])
    return ret


def get_bossnet_boss(names, bossid=None, maxlvl=1):
    # get boss network from boss name
    if not bossid:
        names = list(getbosslike(names))
    else:
        names = [getbosskey(names, bossid)]
    g = get_boss_network(names, maxlvl)
    return g


@deprecated
def get_bossesnet(ids, **kwargs):
    # get boss network from company ids
    # fill boss info for export
    targets = list(getcomboss(ids))
    return get_boss_network(target=targets, **kwargs)


def get_network_names(names, maxlvl=None):
    # get network by company's name
    ids = [x['id'] for x in getidlike(names)]
    if not maxlvl:
        maxlvl = 1
    return get_network(ids, maxlvl=maxlvl)


def get_network_boss(name=None, target=None, **kwargs):
    # get network by boss name
    # input:
    #   name: unicode, boss name
    # output: DiGraph

    if not target:
        name = name.replace(w2, u'')
        targets = list(getbosslike(name))
        cond = {'_id': {'$in': targets}}
    else:
        cond = {'_id': ObjectId(target)}
    orgs = getitem(db.bossnode.find(cond, {'_id': 0, 'orgs': 1}), 'orgs')
    orgs = tuple(set(flatten(orgs)))

    g = get_network(orgs, **kwargs)
    fillgrp(g, orgs)
    return g


def get_network_comboss(id, **kwargs):
    # get network by boss in the same company
    ret = db.cominfo.find({'id': id})
    for r in ret:
        targets = getitem(r['boards'], 'target')
    ids = set()
    for r in db.bossnode.find({'_id': {'$in': targets}}):
        ids.update(r['orgs'])

    g = get_network(list(ids), **kwargs)
    fillgrp(g, [ids])
    return g


def get_network_comaddr(id, **kwargs):
    # get network from the same addr
    addr = None
    for r in db.cominfo.find({'id': id}):
        addr = r['addr']

    ids = [r['id'] for r in db.cominfo.find({'addr': addr})]
    g = get_network(ids, **kwargs)
    fillgrp(g, [ids])
    return g


# }}}
# {{{Ranking
def getRanking(data, rankby, n):
    ret = db.ranking.find(
        {'data': data, 'rankby': rankby},
        {'ranks': {'$slice': n}, '_id': 0, 'rankby': 0, 'data': 0})
    for r in ret:
        return r['ranks']
    return 'NULL'

# }}}


def getComNet(ids, maxlvl=1, **kwargs):
    # 逐級建立公司網絡圖 from comivst

    if not hasattr(ids, '__iter__'):
        ids = [ids]

    cond = {}
    cond['death'] = 0

    G = nx.DiGraph()
    map(G.add_node, ids)

    items = {id: 0 for id in ids}

    def subgraph(G, ids, lvl):
        newlvl = lvl + 1
        if not ids:
            return G, [], newlvl

        cond['$or'] = [{'src': {'$in': ids}},
                       {'dst': {'$in': ids}}]
        ret = db.comivst.find(cond, {'_id': 0, 'death': 0})

        for r in it.ifilter(lambda r: not G.has_edge(r['src'], r['dst']),
                            ret):
            if r['src'] not in items:
                items[r['src']] = newlvl
            if r['dst'] not in items:
                items[r['dst']] = newlvl

            r['width'] = 1
            G.add_edge(r.pop('src'), r.pop('dst'), r)

        ids1 = [key1 for key1, lvl1 in it.ifilter(
                lambda x: lvl == x[1], items.iteritems())]
        return G, ids1, newlvl

    G, ids, lvl = reduce(lambda x, y: subgraph(*x),
                         xrange(maxlvl), (G, ids, 0))
    return G


if __name__ == '__main__':
    """"""
