#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 社團財團法人董監事查詢 （修正中）
# 預計合併至 run_board_target.py 裡面

import pymongo
from groups import groups
from pdb import set_trace
from traceback import print_exc
from collections import defaultdict
import itertools as it
from work import *
from utils import *


# 合併不同來源資料庫時，
# 須為各資料加上來源註記 (source)。
#
# 現有資料整合方式
# 1. 整合比對重複董監事名單
#     1. 輸入董監事資料至現有資料庫 (加上 source)
#     2. 紀錄新增加哪些機構單位 -> newids
#     3. 以 newids 為主，逐一找出資料庫中重複董監名單的公司
#     4. 紀錄重複的董監事名稱與公司代號
# 2. 整合現有 bossnode
#     1. 合併 bossnode
#     2. 消除 redundant bossnode


def update_boss():
    """新增董監事資料表"""
    newids = getcoms()
    logger.info('Total foundation count: {0}'.format(len(newids)))

    appitems()
    dup_bossname(newids)

    names = cn.boards1.distinct('name')

    #run_upd_boards(names)
    run_bossnodes(names=names)
    #run_upd_bossedges(newids)


def appitems():
    # append boards data to twcom.boards
    cn1 = init(CONFIG['dstdb'])

    def insb(r):
        r['source'] = 'twfund'
        r['target'] = r['fund']
        r['id'] = r.pop('fund')
        cn1.boards.insert(r)

    def ins(r):
        r['source'] = 'twfund'
        cn1.cominfo.insert(r)

    cond = {'_id': 0}
    [insb(r) for r in cn.boards1.find({}, cond)]
    [ins(r) for r in cn.fundinfo1.find({}, cond)]


def upd_board_target(name, df, grps, cn):
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
                cn.boards.save(r)


def dup_bossname(comids, nlim=2):
    """save any two fund's duplicate names and count by pair"""
    print get_funname()
    cn1 = init(CONFIG['dstdb'])
    #cn1.dupboss.drop()
    #cn1.dupboss.ensure_index([
    #    ('name', 1), ('fund1', 1), ('fund2', 2)], unique=True)
    #cn1.grpconn.drop()

    dic = defaultdict(set)
    ret = cn1.boards.find(
        {}, {'name': 1, 'id': 1, '_id': 0})
    [dic[getcomid(r)].add(r['name']) for r in ret]

    dic = {k: v for k, v in dic.iteritems() if len(v) >= nlim}
    comids1 = [id for id in comids if id in dic]

    passid = set()
    for id1, id2 in it.product(comids1, dic.keys()):
        if ((id1 == id2) or (id2 in passid)):
            continue
        passid.add(id1)
        df1, df2 = dic[id1], dic[id2]
        namedup = df1.intersection(df2)
        if len(namedup) < nlim:
            continue

        dic1 = {
            'source': 'twfund',
            'com1': id1,
            'com2': id2,
            'names': list(namedup),
            'cnt': len(namedup)
            }
        try:
            cn1.dupboss.save(dic1)
        except pymongo.errors.DuplicateKeyError as e:
            print id1, id2
            """"""
        except:
            print_exc()
            set_trace()

        fun = lambda name: insgrpconn(cn1, name, id1, id2)
        map(fun, namedup)


def insgrpconn(cn1, name, id1, id2):
    try:
        cn1.grpconn.insert({
            'source': 'twfund',
            'name': name,
            'com1': id1,
            'com2': id2})
    except pymongo.errors.DuplicateKeyError as e:
        print name, id1, id2
        """"""
    except:
        print_exc()
        set_trace()


def getcomid(r):
    return r.get('id', r.get('fund'))


def grouping(items, grps=None):
    """grouping items with key"""
    if not grps:
        grps = groups()
    [grps.add(getcomid(r)) for r in items]
    return grps


def run_bossnodes(names):
    """refresh all bossnodes"""
    print get_funname()
    if names is None:
        raise Exception(u'empty names')

    #if reset:
    #    cn.bossnode.drop()
    #    cn.bossnode.create_index([('name', 1), ('target', 1)], unique=True)

    cn1 = init(CONFIG['dstdb'])

    step = 10000
    fun = lambda name_chunk: adj_bossnode(name_chunk, cn1)
    map(fun, chunk(names, step))
    print 'Final'


def adj_bossnode(names, cn1):
    """reset boss node by names"""
    def anyin(li0, li1):
        return any([(x in li1) for x in li0])

    ret = cn1.boards.find({'name': {'$in': names}})
    namedic = groupdic(ret, lambda r: r['name'])
    ret = cn1.bossnode.find({'name': {'$in': names}})
    bnodedic = groupdic(ret, lambda r: r['name'])
    grpdic = defaultdict(groups)
    [grpdic[r['name']].add(r['com1'], r['com2']) for r in
        cn1.grpconn.find({'name': {'$in': names}, 'source': 'twfund'})]
    for k, rs in bnodedic.iteritems():
        for r in rs:
            grpdic[k].add(*r['coms'])
    print len(names), len(bnodedic)

    for name, items in namedic.iteritems():
        li = []
        #print name
        bnodes = bnodedic.get(name, [])
        #if len(bnodes) > 0:
        #    set_trace()
        grps = grpdic[name]
        #[grps.add(*bnode['coms']) for bnode in bnodes]
        grouping(items, grps)
        df = groupdic(items, lambda r: getcomid(r))
        upd_board_target(name, df, grps, cn1)
        #li.append([len(b['coms']) for b in bnodes])

        for grp in grps:
            dic = {'name': name,
                   'coms': list(grp)}
            dic['target'] = dic['coms'][0]

            bnode = [b for b in bnodes if anyin(b['coms'], grp)]

            #li.append(['grp count: ', len(grp), len(bnode)])

            if not bnode:
                cn1.bossnode.insert(dic)
                #li.extend(dic['coms'])
            else:
                b0, chg = None, False
                ids = []
                cnt = 0
                for b in bnode:
                    if b['target'] == dic['target']:
                        b0 = b
                        if b['coms'] != dic['coms']:
                            chg = True
                            b0['coms'] = dic['coms']
                    else:
                        ids.append(b['_id'])
                        cnt += len(b['coms'])
                if b0:
                    if chg:
                        cn1.bossnode.save(b0)
                        #li.extend(b0['coms'])
                else:
                    cn1.bossnode.insert(dic)
                    #li.extend(dic['coms'])
                    #set_trace()
                if ids:
                    cn1.bossnode.remove({'_id': {'$in': ids}})

        #sprint(li)


def getcoms():
    """get funds id"""
    condic = {'boardcnt': {'$gt': 1}, 'log_why': {'$in': [u'變更登記', u'設立登記']}}
    return cn.fundinfo1.find(condic, ['name']).distinct('name')


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


