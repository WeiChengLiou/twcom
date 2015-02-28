#!/usr/bin/env python
# -*- coding: utf-8 -*-

from groups import groups
from pdb import set_trace
from traceback import print_exc
from collections import defaultdict, namedtuple
import itertools as it
from work import *
from utils import *


bad_boards = bad_board(cn)


def getnameall(source):
    return cn.boards.find(
        {'name': {'$nin': bad_boards},
         'source': source}).\
        distinct('name')


def update_boss():
    """新增董監事資料表"""
    idsall = getcoms2()
    logger.info('Total company count: {0}'.format(len(idsall)))

    dup_bossname(idsall)

    names = getnameall('twcom')

    run_upd_boards(names)

    dup_boardlist()

    fix_bad_board()
    run_bossnodes(names=names, reset=False)
    run_upd_bossedges(idsall)


def dup_bossname(comids):
    """save any two company's duplicate names and count by pair"""
    print get_funname()
    cn.dupboss.drop()
    cn.grpconn.drop()

    rets = cn.boards.find(
        {'id': {'$in': comids},
         'name': {'$nin': bad_boards}},
        ['id', 'name'])
    dic = defaultdict(set)
    [dic[r['id']].add(r['name']) for r in rets]

    #rets = getdf(cn.boards.find(
    #    {'id': {'$in': comids},
    #     'name': {'$nin': bad_board(cn)}},
    #    ['id', 'name']))

    for (id1, df1), (id2, df2) in it.combinations(dic.iteritems(), 2):
        namedup = df1.intersection(df2)
        if len(namedup) <= 1:
            continue

        dic = {
            'source': 'twcom',
            'com1': id1,
            'com2': id2,
            'names': list(namedup),
            'cnt': len(namedup)
            }
        cn.dupboss.save(dic)

        for name in namedup:
            dic = {
                'source': 'twcom',
                'name': name,
                'com1': id1,
                'com2': id2}
            cn.grpconn.save(dic)

    cn.dupboss.ensure_index([
        ('name', 1), ('com1', 1), ('com2', 2)], unique=True)
    cn.grpconn.ensure_index(
        [('name', 1), ('com1', 1), ('com2', 2)],
        unique=True)


def dup_boardlist():
    """save any two company's duplicate names by name"""
    print get_funname()
    cn.grpconn.drop()
    cn.grpconn.ensure_index(
        [('name', 1), ('com1', 1), ('com2', 2)],
        unique=True)

    ret = cn.dupboss.find()
    for r in ret:
        for name in r['names']:
            dic = {
                'name': name,
                'com1': r['com1'],
                'com2': r['com2']}
            cn.grpconn.save(dic)


def run_upd_boards(names=None):
    """update all boards"""
    step = 1000
    [upd_boards(x) for x in chunk(names, step)]


def upd_boards(names):
    """update boards: integrate id and repr_inst information"""
    repli = []

    """return grouped records by names"""
    condic = {'name': {'$in': names}}
    ret = cn.boards.find(condic)
    dic = groupdic(ret, key=lambda r: r['name'])

    for name, items in dic.iteritems():
        if name == u'':
            continue
        if items is None:
            print 'Error: ', name
            repli.append(name)
            continue

        df = groupdic(items, key=lambda r: r['id'])
        grps = grouping(items)
        upd_board_target(name, df, grps)
    return repli


def upd_board_target(name, df, grps):
    """update boards info"""
    for grp in grps:
        target = None
        for id in grp:
            if target is None:
                target = id

            for r in df[id]:
                # r = df[id]
                if r.get('target') == target:
                    continue
                r['target'] = target
                if '_id' in r:
                    cn.boards.save(r)
                else:
                    cn.boards.insert(r)


def grouping(items, grps=None):
    """grouping items with key"""
    if grps is None:
        grps = groups()
    for r in items:
        id, instid, inst = r['id'], r['repr_instid'], r['repr_inst']
        # print id, instid, inst

        if inst == '':
            grps.add(id)
        else:
            if instid != 0:
                grps.add(id, instid)
            else:
                grps.add(id, inst)
    return grps


def fix_bad_board():
    """reset every bad names'target as id"""
    print get_funname()
    badnames = bad_boards
    for r in cn.boards.find({'name': {'$in': badnames}}):
        r['target'] = r['id']
        cn.boards.save(r)


def run_bossnodes(names=None, reset=False):
    """refresh all bossnodes"""
    print get_funname()
    if reset:
        cn.bossnode.drop()
        cn.bossnode.create_index([('name', 1), ('target', 1)], unique=True)

    step = 10000
    # map(reset_bossnode, chunk(names, step))
    map(reset_bossnode, list(chunk(names, step)))
    print 'Final'


def reset_bossnode(names):
    """reset boss node by names"""
    repli = []
    cond = {'name': {'$in': names},
            'source': 'twcom'}
    ret = cn.boards.find(cond)
    namedic = groupdic(ret, lambda r: r['name'])
    grpdic = defaultdict(groups)
    [grpdic[r['name']].add(r['com1'], r['com2'])
        for r in cn.grpconn.find(cond)]

    for name, items in namedic.iteritems():
        if items is None:
            repli.append(name)
            continue

        grps = grpdic[name]
        grouping(items, grps)
        upd_board_target(name, groupdic(items, lambda r: r['id']), grps)
        # print grps

        for grp in grps:
            dic = {'name': name,
                   'coms': list(grp)}
            dic['target'] = dic['coms'][0]
            cn.bossnode.insert(dic)

    if repli != []:
        reset_bossnodes(repli)


def insComnetBoss():
    """ Define company network by boss name """
    BossId = namedtuple('BossId', ('name', 'id'))
    dic = [BossId(**x) for x in cn.boards.find({},
           {'_id': 0, 'id': 1, 'name': 1}).sort('id')]
    ret = tuple((k, sorted(set(g))) for k, g in it.groupby(dic, lambda x: x.id))
    idset = [x[0] for x in ret]

    #for ids in chunk(idset,100):


    for x, y in it.combinations(ret, 2):
        cnt = len(x[1].intersection(y[1]))
        tot = len(x[1] | y[1])
        if cnt > 0:
            dic = {'src': x[0],
                   'dst': y[0],
                   'cnt': cnt,
                   'ratio': float(cnt) / tot}
            cn.ComnetBoss.insert(dic)


def getcoms2():
    """get coms id"""
    condic = {'boardcnt': {'$gt': 1}, 'status': {'$regex': u'核准'}}
    return cn.cominfo.find(condic, ['id']).distinct('id')


def run_upd_bossedges(ids=None):
    """update all boss edges"""
    print get_funname()
    cn.bossedge.drop()
    cn.bossedge.ensure_index([('src', 1), ('dst', 1)])
    map(upd_bossedge, chunk(ids, 500))


def upd_bossedge(ids):
    """update boss edge by ids"""
    def getkey(r):
        return (r['name'], r['target'] if 'target' in r else r['id'])

    def inskey(key0, key1):
        try:
            dic = {'src': u'\t'.join(key0),
                   'dst': u'\t'.join(key1),
                   'cnt': len(mandic[key0].intersection(mandic[key1]))}
            cn.bossedge.insert(dic)
        except:
            print_exc()
            set_trace()

    bossdic = {}
    ret = cn.boards.find({
        'id': {'$in': ids},
        '$or': [{'no': '0001'}, {'title': u'董事長'}]
        })
    [bossdic.__setitem__(r['id'], getkey(r)) for r in ret]

    #mandic = {(r['name'], r['target']): set(r['coms']) for r in
    #          cn.bossnode.find({'coms': {'$in': ids}})}
    mandic = {}
    for r in cn.bossnode.find({'coms': {'$in': ids}}):
        mandic[(r['name'], r['target'])] = set(r['coms'])

    ret = cn.boards.find({
        'id': {'$in': ids}})
    for r in ret:
        key0 = getkey(r)
        if key0 not in mandic:
            continue
        if r['id'] not in bossdic:
            ret = cn.boards.find({
                'id': r['id'],
                'title': {'$ne': u'法人代表'}}).sort('no')
            for r in ret:
                bossdic[r['id']] = getkey(r)
                break
            if r['id'] not in bossdic:
                bossdic[r['id']] = [getkey(r)]

        if key0 == bossdic[r['id']]:
            continue
        key1 = bossdic[r['id']]
        if key1 not in mandic:
            continue
        if isinstance(key1, list):
            for key in key1:
                inskey(key0, key)
                inskey(key, key0)
            bossdic[r['id']].append(key0)
        else:
            inskey(key0, key1)


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

