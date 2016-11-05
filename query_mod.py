#!/usr/bin/env python
# -*- coding: utf-8 -*-
output_path = 'output'
urlpath = output_path

import pandas as pd
import matplotlib as mpl
import matplotlib.pylab as plt
import networkx as nx
from collections import defaultdict

font = 'AR PL KaitiM Big5'
mpl.rcParams['font.family'] = font  # 設定中文字體
mpl.rcParams['font.sans-serif'] = font  # 設定中文字體

# %matplotlib inline

import seaborn as sns
sns.set(font=font)  # 設定中文字體
sns.set_context('poster')
size_poster = mpl.rcParams['figure.figsize']
# mpl.rcParams['figure.figsize'] = 16, 12

from twcom import query
from twcom import utils
db = query.db


def getboardcnt(id):
    ret = db.cominfo.find({'id': id})
    for r in ret:
        return r['boardcnt']
    return 0


def getboardcnt_dic(ids):
    ret = db.cominfo.find({'id': {'$in': ids}}, ['id', 'boardcnt'])
    if ret:
        return {k['id']: k['boardcnt'] for k in ret}
    return {}


def getchild(mainid):
    ids = [mainid]
    li, namedic = [], {}
    for id0 in ids:
        ret = tuple(db.comivst.find({'src': id0, 'death': 0}))
        idli = [id0]
        idli.extend([r['dst'] for r in ret])
        namedic.update(utils.getnamedic(idli))
        boardic = getboardcnt_dic(idli)

        for r in ret:
            id = r['dst']
            if (id not in ids):  # and (ratio>=50):
                li.append((namedic[id0], namedic[id],
                           boardic[id], int(r['seat']),
                          r['seatratio']))
                ids.append(id)
    if li:
        li = pd.DataFrame(li,
                          columns=['src', 'dst', 'cnt', 'seat', 'seatratio'])
    return ids, li


def getparent(mainid):
    ids = [mainid]
    li, namedic = [], {}
    for id0 in ids:
        ret = tuple(db.comivst.find({'dst': id0, 'death': 0}))
        idli = [id0]
        idli.extend([r['src'] for r in ret])
        namedic.update(utils.getnamedic(idli))
        boardic = getboardcnt_dic(idli)
        for r in ret:
            id = r['src']
            if (id not in ids):  # and (ratio>=50):
                li.append((namedic[id], namedic[id0],
                           boardic[id0], int(r['seat']),
                          r['seatratio']))
                ids.append(id)

    if li:
        li = pd.DataFrame(li, columns=[
            'src', 'dst', 'cnt', 'seat', 'seatratio'])
    return ids, li


def fillgrp(g, grps):
    for k, v in g.node.iteritems():
        v['group'] = 0
        for i, grp in enumerate(grps):
            if k in grp:
                v['group'] = i + 1
                break


def filledge_com(g):
    ids = tuple(g.node.keys())
    ret = db.comivst.find({
        'src': {'$in': ids},
        'dst': {'$in': ids},
        'death': 0,
    })
    dic = {}
    for r in ret:
        dic[(r['src'], r['dst'])] = r['seatratio']/10
    for src, vsrc in g.edge.iteritems():
        for dst, v in vsrc.iteritems():
            key = (src, dst)
            v['weight'] = dic.get(key, 1)
            # print key, v['weight']


def iters(arg1, arg2):
    for x in arg1:
        yield x
    for x in arg2:
        yield x


def iscontrol(id, ids, names):
    # 檢查是否有絕對控制權
    cnt, totcnt = 0, 0
    for boss in db.boards.find({'id': id, 'title': {'$ne': u'法人代表'}}):
        totcnt += 1
        if (boss['name'] in names) or (boss['repr_instid'] in ids):
            # 核心成員以個人投資或是法人投資均計入控制席次
            cnt += 1

    ratio = round(float(cnt) / totcnt * 100, 2)
    return ratio >= 50, cnt, ratio


def getmainchild(mainid):
    # 查詢核心企業
    if hasattr(mainid, '__iter__'):
        ids = list(mainid)
    else:
        ids = [mainid]
    names = set(db.boards.find(
        {'id': {'$in': ids}}).distinct('name'))  # 取得核心成員名單
    li, namedic = [], {}
    for src in ids:
        waitli = []
        retli = tuple(db.comivst.find({'src': src, 'death': 0}))
        iter1 = iters(retli, waitli)
        idli = [src]
        idli.extend([r['dst'] for r in retli])
        namedic.update(utils.getnamedic(idli))
        boardic = getboardcnt_dic(idli)

        for r in iter1:
            # 查詢每個 src 投資的公司
            dst, ratio = r['dst'], r['seatratio']
            if dst in ids:
                continue

            app, cnt, ratio = iscontrol(dst, ids, names)
            if app:
                li.append((namedic[r['src']], namedic[dst],
                           boardic[dst], cnt, ratio))
                ids.append(dst)
                # 新增具絕對控制權公司的董監事名單為核心成員
                names.update(db.boards.find({'id': dst}).distinct('id'))
            elif r not in waitli:
                # 不具絕對控制權，但可能尚有其他間接投資，故稍候再檢查一次
                waitli.append(r)

    if li:
        li = pd.DataFrame(li, columns=[
            'src', 'dst', 'cnt', 'seat', 'seatratio'])
    return list(ids), li


def draw_scatter(g, sizefun=None, lblfun=None):
    fig, ax = plt.subplots(1)
    if sizefun:
        s = map(sizefun, g.node.values())
    else:
        s = [10 for _ in xrange(len(g.node))]

    deg = nx.degree(g)
    x = [deg[k] for k in g.node]

    betw = nx.closeness_centrality(g.to_undirected())
    y = [betw[k] for k in g.node]

    c = [v.get('group', 0) for v in g.node.values()]
    ax.scatter(x, y, c=c, cmap=plt.cm.jet, vmin=min(c),
               vmax=max(c), s=s, alpha=0.3)

    if lblfun:
        dic = defaultdict(list)
        for i, (k, v) in enumerate(g.node.iteritems()):
            pos = (x[i], round(y[i], 2))
            if lblfun(pos):
                # if (pos[0]>=5 and pos[1]>=0.35) or
                #     len(v.get('titles', 0))>=5:
                dic[pos].append(v['name'])

        for pos, v in dic.iteritems():
            ax.text(pos[0], pos[1], u'\n'.join(v), fontsize=16)


def BoardStat(id, srcs):
    # get board stat (seat, seat ratio) given srcs
    coms = db.boards.find(
        {'repr_inst': {'$in': srcs}},
        {'_id': 0, 'id': 1}
        )
    coms = [r['id'] for r in coms]


if __name__ == '__main__':
    """"""
