#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gzip
from pdb import set_trace
from traceback import print_exc
from collections import defaultdict, Counter
import cPickle
import itertools as it
from work import *
from utils import *
from twcom.sparse_mat import sparse_mat as sm
from twcom.groupset import groupset
import networkx as nx


com_cond = {'boardcnt': {'$gt': 1}, 'status': {'$regex': u'核准'}}
tmpid = "00000016", u'王振林'


def resetComnetBoss():
    global bad_boards
    bad_boards = cPickle.load(open('bad_boards.pkl', 'rb'))

    badcoms = getbadcoms()
    obj, G = setComnetBoss(badcoms)
    cPickle.dump((obj, G, badcoms), gzip.open('test1.pkl', 'wb'))
    #obj, G, badcoms = cPickle.load(gzip.open('test.pkl', 'rb'))
    updivst(obj, G, badcoms)
    insComBosslink(G)
    bossdic = makeboss(obj, G)
    cPickle.dump((bossdic, ), gzip.open('test2.pkl', 'wb'))
    #bossdic = cPickle.load(gzip.open('test2.pkl', 'rb'))[0]
    updboards(obj, G, bossdic)


def setBadBoards():
    bad_boards = bad_board(db)
    cPickle.dump(bad_boards, open('bad_boards.pkl', 'wb'))


def test():
    """"""
    # badcoms = getbadcoms()
    # obj, G = setComnetBoss(badcoms)
    # print 'write'
    # cPickle.dump((obj, G, badcoms), gzip.open('test.pkl', 'wb'))
    # print 'write done'
    # obj, G, badcoms = cPickle.load(gzip.open('test.pkl', 'rb'))
    # cond = {'id': '84149630'}
    # updivst(obj, G, badcoms, cond)


def setComnetBoss(badcoms):
    print get_funname()
    obj = sm()
    ret = db.cominfo.find({}, {'id': 1, 'boards': 1})
    for r in ret:
        try:
            addBoard(obj, r['id'], getitem(r['boards'], 'name'))
            # if r['id'] == tmpid[0]:
            #     print obj.xdic[tmpid[0]], obj.ydic[tmpid[1]]
            #     set_trace()
        except:
            print_exc()
            set_trace()

    G = nx.DiGraph()
    for l in obj.links:
        names = obj.intersec(*l)
        if len(names) >= 2:
            dic = {
                'bossCnt': len(names),
                'bossX': list(names),
                'bossJac': obj.jaccard(*l),
                'death': sum([(il in badcoms) for il in l]),
                'ivst': 0
                }
            G.add_edge(l[0], l[1], dic)
            G.add_edge(l[1], l[0], dic)

    return obj, G


def addBoard(obj, instid, names):
    f1 = lambda name: name not in bad_boards
    obj.add(instid, list(it.ifilter(f1, names)))


def chkBoard(obj, instid, names, badcoms):
    f1 = lambda name: name not in bad_boards
    f2 = lambda name: name not in obj.xdic[instid]
    f = lambda name: f1(name) and f2(name)
    names = list(it.ifilter(f, names))
    if names:
        print u','.join(names)
        addBoard(obj, instid, names)
        insReprInfo(instid, names)


def insReprInfo(id, names):
    boards = []
    for name in names:
        dic = {'name': name,
               'title': u'法人代表',
               'repr_inst': u'',
               'repr_instid': 0,
               'equity': 0}
        boards.append(dic)

    chg = 0
    ret = db.cominfo.find({'id': id})
    try:
        for r in ret:
            r['boards'].extend(boards)
            db.cominfo.save(r)
            chg = 1

        if chg == 0:
            dic = {'id': id,
                   'name': id,
                   'boards': boards}
            db.cominfo.insert(dic)
    except:
        print_exc()
        set_trace()


def updivst(obj, G, badcoms, cond=None):
    print get_funname()
    # update inst ivst information

    def update(r):
        id = r['id']
        reprs = groupdic(r['boards'], lambda r1: (r1['repr_instid'], r1['repr_inst']))
        for (instid, inst), rs in reprs.iteritems():
            l = [instid if unicode(instid) != u'0' else inst]
            if l[0] == u'':
                continue
            chkBoard(obj, l[0], getitem(rs, 'name'), badcoms)

            l.append(id)
            dic = G.get_edge_data(*l)
            if dic is None:
                names = obj.intersec(*l)
                if not names:
                    continue
                ivstnames = getitem(r['boards'], 'name')
                G.add_edge(l[0], l[1],
                    {
                    'ivst': 1,
                    'bossCnt': len(names),
                    'bossX': list(names),
                    'bossJac': obj.jaccard(*l),
                    'death': sum([(il in badcoms) for il in l]),
                    'ivstCnt': len(ivstnames),
                    'ivstRatio': float(len(ivstnames)) / len(obj.xdic[id])
                    })
            else:
                ivstnames = getitem(r['boards'], 'name')
                dic['ivst'] = 1
                dic['ivstcnt'] = len(ivstnames)
                dic['ivstRatio'] = float(len(ivstnames)) / len(obj.xdic[id])

    if not cond:
        cond = {}
    ret = db.cominfo.find(cond, {'id': 1, 'boards': 1, '_id': 0})
    map(update, ret)


def insComBosslink(G):
    print get_funname()
    db.ComBosslink.drop()
    for x in G.edges():
        dic = {'src': x[0], 'dst': x[1]}
        dic.update(G.get_edge_data(*x))
        db.ComBosslink.insert(dic)


def makeboss(obj, G):
    print get_funname()
    db.bossnode.drop()

    bossdic = defaultdict(groupset)
    for k, v in obj.ydic.iteritems():
        map(bossdic[k].add, v)
    for l in G.edges():
        names = obj.intersec(*l)
        for name in names:
            bossdic[name].add(*l)

    #bossobj = sm()
    for k, vs in bossdic.iteritems():
        # print k, v
        for v in vs:
            doc = {'name': k, 'orgs': list(v)}
            db.bossnode.insert(doc)
            setattr(v, '_id', doc['_id'])
            #bossobj.adddic(doc['_id'], doc, 'orgs')

    return bossdic#, bossobj


@deprecated
def insbosslink(bossobj):
    db.bosslink.drop()
    set_trace()
    for l in bossobj.links:
        orgs = bossobj.intersec(*l)
        doc = {'link': l, 'orgs': list(orgs), 'cnt': len(orgs), 'jaccard': bossobj.jaccard(*l)}
        db.bosslink.insert(doc)


def updboards(obj, G, bossdic):
    print get_funname()
    ret = db.cominfo.find({'boardcnt': {'$gt': 0}})
    fun = lambda boss: boss['name'] not in bad_boards
    for r in ret:
        for boss in r['boards']:
            if fun(boss):
                try:
                    id, name = r['id'], boss['name']
                    boss['target'] = bossdic[name].getgrp(id)._id
                except:
                    print_exc()
                    set_trace()
            else:
                boss['target'] = None
        db.cominfo.save(r)


if __name__ == '__main__':
    names = [u'王文洋', u'余建新', u'羅智先', u'謝國樑', u'王貴雲', u'王雪紅']
    ids = [u'75370905', u'16095002', u'73251209', u'75370601']

    #run_upd_boards()
    #update_boss()

    #idsall = getcoms2()
    #dup_bossname(idsall)
    #dup_boardlist()
    #comids = ['89399262', '79837539']
    #dup_bossname(comids)
    #reset_bossnode()

