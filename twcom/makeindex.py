#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import sys
import gzip
import os
import json
import itertools as it
from traceback import print_exc
from pdb import set_trace
from collections import defaultdict
import yaml
from datetime import datetime
from pymongo.errors import DuplicateKeyError
import time
from work import yload, replaces, ysave, get_funname, chunk, groupdic
from work import deprecated, trytest
from utils import db, logger, getbadcoms
CONFIG = yaml.load(open('config.yaml'))
path = 'files'
path = 'TW Company Download/files'
path = '{0}/files'.format(CONFIG['src'])
print path
errcol = {}
errid = {}


# 取得 json 欄位定義
filedic = yaml.load(open('filespec.yaml'))

# 設定想留在資料庫的欄位
tblcol = yaml.load(open('tblcol.yaml'))


fis = sorted(list(it.ifilter(
    lambda x: ('bussiness' not in x) and ('0000' in x),
    os.listdir(path))))
fis1 = it.ifilter(lambda x: x[0] == '0', fis)


class adjname(object):
    fixwordli = yload('doc/fixword.yaml')

    @staticmethod
    def run(v):
        return reduce(lambda x, y: x.replace(*y),
                      adjname.fixwordli.iteritems(), v)


# 每個 json item 可分為以下類別
# 獨資企業： indinfo
# 本土分公司： branchinfo
# 外資在臺註冊分公司： fbranchinfo
# 外資在臺註冊辦事處：fagentinfo
# 本土或外資在台註冊企業： baseinfo


class ComItem(object):
    def __init__(self, kv):
        self.id = kv[0]

#         fkeys = (
#             u'在中華民國境內營運資金',
#             u'在台灣地區營業所用',
#         )

        # 初次對 json 分類為不同類別
        if u'組織類型' in kv[1]:
            self.tbl = 'indinfo'
            self.read(kv, filedic['indinfo'])
        elif ((u'分公司名稱' in kv[1]) and (u'公司名稱' in kv[1])) or\
                (u'核准認許日期' in kv[1]):
            self.tbl = 'fbranchinfo'
            self.read(kv, filedic['fbranchinfo'])
        elif u'辦事處所在地' in kv[1]:
            self.tbl = 'fagentinfo'
            self.read(kv, filedic['fagentinfo'])
        elif u'分公司名稱' in kv[1]:
            self.tbl = 'branchinfo'
            self.read(kv, filedic['branchinfo'])
        else:
            self.tbl = 'baseinfo'
            self.read(kv, filedic['baseinfo'])

    def read(self, kv, coldic):
        errkey = []
        for k, v in kv[1].iteritems():
            key = coldic.get(k)
            if not key:
                # 遇到異常欄位就要先註記再即時 debug
                if k not in errcol.keys():
                    # 列出異常欄位名稱與公司統編
                    print u'Unknown Column:', kv[0], self.tbl, k
                    errcol[k] = (self.id, v)
                if not errid.get(self.id):
                    errid[self.id] = (self.id, kv[1])
                errkey.append(key)
            else:
                self.process(key, v)
        if errkey:
            set_trace()

    def process(self, key, v):
        if key == 'name':
            if isinstance(v, list) and len(v) == 1:
                v = v[0]
            if isinstance(v, list):
                if isinstance(v[1], list):
                    self.name1 = v[1][0]
                    self.name = v[0][0]
                else:
                    self.name1 = v[1]
                    self.name = v[0]
            else:
                self.name = v
            self.name = adjname.run(self.name)
            return
        elif key == 'boards':
            for boss in v:
                boss[u'職稱'] = replaces(
                    boss[u'職稱'], ('\r\n', ' ', 'null', '\t'))
                try:
                    boss[u'出資額'] = int(replaces(boss[u'出資額'], (',',)))
                except:
                    boss[u'出資額'] = 0
        elif 'date' in key and v:
            v = datetime(v['year'], v['month'], v['day'])
        elif key in ('capital', 'realcap'):
            try:
                v1 = v.replace(',', '')
                val = float(v1) if v1 != '' else 0
                self.__dict__[key] = val
                return
            except:
                print_exc()
                set_trace()
        elif key == 'status':
            v = replaces(v, (u' ', u'　', 'null'))

        self.__dict__[key] = v if not isinstance(v, basestring) else v.strip()

    def insinfo(self):
        # 輸入公司基本資料，每間公司有自己的類別
        items = {'type': self.tbl,
                 'source': 'twcom'}
        for col in tblcol[self.tbl]:
            if not hasattr(self, col):
                continue
            try:
                items[col] = self.__dict__[col]
            except:
                print_exc()
                set_trace()

        items['boards'] = self.getboards()
        items['boardcnt'] = len(items['boards'])

        try:
            db.cominfo.insert(items)
        except:
            print_exc()
            set_trace()

    def getboards(self):
        # 取得董監事資料
        li = []

        if 'boards' not in self.__dict__:
            return li
        boardic = filedic['boards']

        # 各公司董監事名單使用 set 儲存，因為可能有重複
        sets = set()
        cnt = 0
        for boss in self.boards:
            vs = {}
            for col, v in boss.iteritems():
                if col == u'所代表法人':
                    if isinstance(v, list):
                        vs['repr_inst'] = v[1]
                        vs['repr_instid'] = v[0]
                    else:
                        vs['repr_inst'] = v
                        vs['repr_instid'] = '0'
                    vs['repr_inst'] = adjname.run(vs['repr_inst'])
                else:
                    vs[boardic[col]] = v if not isinstance(v, basestring)\
                        else v.strip()

            vs['name'] = adjname.run(vs['name'])
            kvs = tuple([vs[k] for k in (
                'name', 'repr_inst', 'repr_instid')])
            if kvs not in sets:
                sets.add(kvs)

                try:
                    li.append(vs)
                    cnt += 1
                except:
                    print_exc()
                    set_trace()

        return li

    def insboards(self):
        # 輸入董監事資料
        if 'boards' not in self.__dict__:
            return
        boardic = filedic['boards']

        # 各公司董監事名單使用 set 儲存，因為可能有重複
        sets = set()
        cnt = 0
        for boss in self.boards:
            vs = {'id': self.id,
                  'source': 'twcom'}
            for col, v in boss.iteritems():
                if col == u'所代表法人':
                    if isinstance(v, list):
                        vs['repr_inst'] = v[1]
                        vs['repr_instid'] = v[0]
                    else:
                        vs['repr_inst'] = v
                        vs['repr_instid'] = '0'
                    vs['repr_inst'] = adjname.run(vs['repr_inst'])
                else:
                    vs[boardic[col]] = v if not isinstance(v, basestring)\
                        else v.strip()

            vs['name'] = adjname.run(vs['name'])
            kvs = tuple([vs[k] for k in (
                'id', 'name', 'repr_inst', 'repr_instid')])
            if kvs in sets:
                # 另外紀錄董監事名單裡重複的名字
                with open('boards_dbl.csv', 'a') as f:
                    f.write(u','.join(map(unicode, kvs)).encode('utf8'))
                    f.write(u'\n'.encode('utf8'))
                continue
            else:
                sets.add(kvs)

                try:
                    db.boards.insert(vs)
                    cnt += 1
                except:
                    print_exc()
                    set_trace()

        self.boardcnt = cnt


def getfile(dst):
    print "Please goto 'http://gcis.nat.g0v.tw/'"


def instbl(*kv):
    # 處理每筆 json item 至資料庫
    obj = ComItem(kv)
    obj.insboards()
    obj.insinfo()


def tblcnt(kv):
    # 對每筆 json items 統計總樣本數
    global comcnt
    comcnt = 0
    obj = ComItem(kv)
    if 'boardcnt' in obj.__dict__ and obj.boardcnt > 0 and u'核准' in obj.status:
        comcnt += 1


def readg(fi):
    # Read gzip file
    def dicfun(li):
        # decode line into key-value pair
        return li[:8], json.loads(li[9:])

    try:
        g = gzip.open(os.path.join(path, fi))
        for li in g:
            yield dicfun(li)
    except:
        print sys.exc_info()
    finally:
        print 'close %s' % fi
        g.close()


def runjobs(*args):
    # 逐檔案、逐 json item 執行給定函數
    kvs = it.chain.from_iterable(it.imap(readg, fis))

    def f1(kv):
        return map(lambda fun: fun(kv), args)

    map(f1, kvs)


def insraw():
    # Insert raw data
    tbl = 'raw'
    db[tbl].drop()
    coll = db[tbl]

    def fun(kv):
        kv[1]['id'] = kv[0]
        return (kv[1])
    kvs = it.chain.from_iterable(it.imap(readg, fis))

    li = []
    for x in it.imap(fun, kvs):
        li.append(x)
        if len(li) == 50000:
            coll.insert_many(li)
            li = []
    if li:
        coll.insert_many(li)


def refresh():
    # 新增、處理資料庫

    # cn = init(CONFIG['db'])
    db.cominfo.drop()
    db.cominfo.ensure_index([('id', 1)], unique=True)
    db.boards.drop()
    db.boards.ensure_index(
        [
            ('id', 1), ('name', 1),
            ('repr_instid', 1), ('repr_inst', 1),
            ('title', 1), ('equity', 1)],
        unique=True)
    f = open('boards_dbl.csv', 'w')
    f.close()
#     runjobs(instbl)

    def fun(kv):
        id = kv['id']
        del kv['id']
        instbl(id, kv)

    map(fun, db.raw.find({}, {'_id': 0}))

    # fixing()


def genbadstatus():
    # return bad company status
    status = set()
    status.update(db.cominfo.find(
        {'status': {'$not': re.compile(u'核准')}}).distinct('status'))
    status.update(db.cominfo.find(
        {'$or': [{'status': {'$regex': u'停業'}},
         {'status': {'$regex': u'解散'}}]}).distinct('status'))
    ysave(sorted(status), 'doc/badstatus.yaml')


def fixing1():
    ins_iddic()
    fixdata1()
    cnt = 1
    while cnt > 0:
        cnt = fixboards1()


def fixing():
    # Preprocessing

    ins_iddic()
    # 修正董監事資料錯別字
    fixdata()

    for i in range(20):
        # 修正董監事名單（母函數）
        # 設置迴圈是因為有可能需要檢查兩次以上
        cnt = fixboards()
        if cnt == 0:
            break
        time.sleep(10)

    if cnt != 0:
        logger.info('Ignore comivst insert process')
    else:
        ins_comivsts()
        # rm_badcomivst()

    # if nations:
    #     cPickle.dump((nations,), open('dics.dat', 'wb'), True)


def defnation():
    # 找出國家別
    nations = set()
    tbls = ('fbranchinfo', 'fagentinfo')
    for r in db.cominfo.find({'type': {'$in': tbls},
                              'name': {'$regex': u'商'}},
                             ['name']):
        name = r['name']
        i = name.index(u'商')
        nations.add(name[:(i+1)])
    nations = list(nations)
    print len(nations)
    return nations


def ins_iddic():
    # 新增公司名稱與統編對照（僅留下經營中公司）
    print get_funname()
    db.iddic.drop()

    def ins_update(name, id):
        iddic[name].append(id)
        # ret = db.iddic.find_one({'name': name})
        # if ret:
        #     ret['id'].append(id)
        #     db.iddic.save(ret)
        # else:
        #     db.iddic.insert({'name': name, 'id': [id]})

    iddic = defaultdict(list)
    bads = yload('doc/badstatus.yaml')
    dic1 = {'type': {'$in': ['baseinfo', 'fbranchinfo', 'fagentinfo']},
            'status': {'$nin': bads}}
    coldic = {'name': 1, 'id': 1, 'status': 1, 'name1': 1}

    for r in db.cominfo.find(dic1, coldic):
        ins_update(r['name'], r['id'])
        if 'name1' in r:
            ins_update(r['name1'], r['id'])
    db.iddic.insert(
        [{'name': k, 'id': v} for k, v in iddic.iteritems()])
    db.iddic.ensure_index([('name', 1)])


def ins_comivsts():
    # 新增公司連結母函數
    badcoms = getbadcoms()
    db.comivst.drop()
    db.comivst.ensure_index([
        ('src', 1), ('dst', 1)])
    comall = set()
    [comall.add(r['id']) for r in db.boards.find()]
    map(lambda coms: ins_comivst(coms, badcoms),
        chunk(list(comall), 200))


def ins_comivst(coms, badcoms):
    # 新增公司連結子函數
    seatdic = {
        k['id']: k['boardcnt'] for k in db.cominfo.find(
            {'boardcnt': {'$gt': 0},
             'id': {'$in': coms}},
            ['id', 'boardcnt'])}
    query = {'id': {'$in': coms},
             'repr_inst': {'$ne': ''}}

    chkdic = defaultdict(set)
    dic = groupdic(db.boards.find(query,
                   ['id', 'name', 'repr_instid', 'repr_inst']),
                   lambda r: (r['id'], r['repr_instid'], r['repr_inst']))
    for (dstid, instid, inst), rs in dic.iteritems():
        cnt = len(rs)
        if dstid in seatdic:
            cntr = round(float(cnt)/seatdic[dstid]*100, 2)
        else:
            cntr = None
        if instid != 0:
            srcid = instid
        else:
            srcid = inst

        items = {
            'src': srcid,
            'dst': dstid,
            'seat': cnt,
            'seatratio': cntr,
            'death': sum([(il in badcoms) for il in (srcid, dstid)]),
            }
        db.comivst.insert(items)

        chkdic[srcid].update([r['name'] for r in rs])

    fill_hideboss(chkdic)


def get_degree(db):
    # 取得連結數資訊
    dic = defaultdict(dict)
    coldic = {('$src', 'ncom'): 'ncom_out',
              ('$src', 'nseat'): 'nseat_out',
              ('$dst', 'ncom'): 'ncom_in',
              ('$dst', 'nseat'): 'nseat_in'}

    def update(r, tbl):
        d = dic[r.pop('_id')]
        [d.__setitem__(coldic[(tbl, k)], v) for k, v in r.iteritems()]

    for col in ('$src', '$dst'):
        ret = db.comivst.aggregate([{'$group': {
            '_id': '$src',
            'ncom': {'$sum': 1},
            'nseat': {'$sum': '$seat'}}}])['result']
        map(lambda r: update(r, col), ret)

    return dic


def fill_hideboss(chkdic):
    # 新增各公司非董監事的法人代表
    for r in db.boards.find({'id': {'$in': chkdic.keys()}}):
        if r['id'] in chkdic and r['name'] in chkdic[r['id']]:
            chkdic[r['id']].remove(r['name'])

    # lock = 0
    # reprdic = defaultdict(list)
    for id, names in chkdic.iteritems():
        for name in names:
            dic = {'id': id,
                   'name': name,
                   'title': u'法人代表',
                   'repr_inst': u'',
                   'repr_instid': 0,
                   'target': id,
                   'equity': 0}
            db.boards.save(dic)

    #         del(dic['id'], dic['target'], dic['_id'])
    #         reprdic[id].append(dic)

    # ret = db.cominfo.find({'id': {'$in': reprdic.keys()}})
    # for r in ret:
    #     r['boards'].extend(reprdic.pop(r['id']))
    #     r['boardcnt'] = len(r['boards'])
    #     db.cominfo.save(r)
    # for id, boards in reprdic.iteritems():
    #     dic = {'id': id,
    #            'name': id,
    #            'boards': boards
    #            }
    #     db.cominfo.save(dic)


@deprecated
def rm_badcomivst():
    # 移除公司連結裡非營業中的企業。
    try:
        bads = yload('doc/badstatus.yaml')

        coms = set(db.comivst.distinct('src'))
        coms.update(db.comivst.distinct('dst'))
        dic = {'id': {'$in': list(coms)},
               'status': {'$in': list(bads)}}
        badcoms = db.cominfo.find(dic).distinct('id')
        print 'badcoms count: {0}'.format(len(badcoms))
        db.comivst.remove({'src': {'$in': badcoms}})
        db.comivst.remove({'dst': {'$in': badcoms}})
    except:
        print_exc()
        set_trace()


def update(collection, condition, setval, errfun=None):
    for x in collection.find(condition):
        for k, v in setval.iteritems():
            x[k] = v
        while 1:
            try:
                collection.save(x)
                break
            except DuplicateKeyError:
                if errfun:
                    errfun(x)
                else:
                    return x


# Clean Data
def fixboards():
    # 修正董監事名單（母函數）
    condition = {'repr_inst': {'$ne': ''}}
    reprids = db.boards.find(condition).distinct('repr_inst')
    step = 100
    totcnt = len(reprids) / 200 + 1
    logger.info('fixboards:  Total Count - {0}'.format(len(reprids)))
    toterr = 0
    for i, x in enumerate(chunk(reprids, step)):
        logger.info('fixboards: {0} / {1}'.format(i, totcnt))
        errcnt, li = fixboard(x)
        toterr += errcnt
    print 'update {0} records'.format(toterr)
    return toterr


def fixboards1():
    # 修正董監事名單（母函數）
    condition = {'repr_inst': {'$ne': ''}}
    reprids = db.boards.find(condition).distinct('repr_inst')
    step = 100
    totcnt = len(reprids) / 200 + 1
    logger.info('fixboards:  Total Count - {0}'.format(len(reprids)))
    toterr = 0
    for i, x in enumerate(chunk(reprids, step)):
        logger.info('fixboards: {0} / {1}'.format(i, totcnt))
        errcnt = fixboard1(x)
        toterr += errcnt
    print 'update {0} records'.format(toterr)
    return toterr


def fixboard(names):
    # 修正董監事名單（子函數）
    cnt, li = 0, []

    def upd_item(collections, item, truedic):
        [item.__setitem__(k, v) for k, v in truedic.iteritems()]
        while 1:
            try:
                collections.save(item)
                break
            except:
                item['equity'] += 1

    dic = defaultdict(list)
    for r in db.boards.find({'repr_inst': {'$in': names}}):
        key = r['repr_instid'], r['repr_inst']
        dic[key].append(r)

    for key, items in dic.iteritems():
        id, name = key

        try:
            if id and (len(id) == 8):
                # 檢查法人代表裡公司名稱是否與基本資料相符
                ret = db.cominfo.find_one({'id': id}, ['id', 'name'])
                if ret and name != ret['name']:
                    [upd_item(db.boards,
                              item,
                              {'repr_inst': ret['name']})
                        for item in items]
                    li.append((id, name, ret['name']))
                    cnt += 1
            else:
                # 從法人代表公司名稱反查法人代表統編
                ret = db.iddic.find_one({'name': name})
                if ret is None:
                    continue

                ids = ret['id']
                if len(ids) == 1:
                    [upd_item(db.boards,
                              item,
                              {'repr_instid': ids[0]})
                        for item in items]
                    li.append((name, id, ids[0]))
                    cnt += 1
                else:
                    # 相同公司名稱有兩間以上，不處理
                    logger.warning(
                        u'Duplicate name: {0} {1} -> {2}'.format(
                            id, name, u' '.join(ids)))
        except:
            print_exc()
            set_trace()

    return cnt, li


@trytest
def fixboard1(names):
    # 修正董監事名單（子函數）
    li = {}
    ret0 = db.cominfo.find({'boards.repr_inst': {'$in': names}})

    for r in ret0:
        chg = 0
        dic = groupdic(r['boards'],
                       lambda r1: (r1['repr_instid'], r1['repr_inst']))
        for key, items in dic.iteritems():
            id, name = key

            if id and (len(id) == 8):
                # 檢查法人代表裡公司名稱是否與基本資料相符
                try:
                    ret = db.cominfo.find_one({'id': id}, ['id', 'name'])
                    if ret and name != ret['name']:
                        chg = 1
                        [item.__setitem__('repr_inst', ret['name'])
                            for item in items]
                except:
                    print_exc()
                    set_trace()
            else:
                # 從法人代表公司名稱反查法人代表統編
                ret = db.iddic.find_one({'name': name})
                if ret is None:
                    continue

                ids = ret['id']
                if len(ids) == 1:
                    chg = 1
                    [item.__setitem__('repr_instid', ids[0])
                        for item in items]
                else:
                    # 相同公司名稱有兩間以上，不處理
                    logger.warning(
                        u'Duplicate name: {0} {1} -> {2}'.format(
                            id, name, u' '.join(ids)))
        if chg == 1:
            li[r['id']] = r

    if li:
        map(db.cominfo.save, li.values())
    return len(li)


def fixdata():
    # 修正董監事資料錯別字
    fi = 'doc/fix_board.yaml'
    for k, v in yload(fi).iteritems():
        v = adjname.run(v)
        try:
            assert(isinstance(k, unicode))
            assert(isinstance(v, unicode))
        except:
            print_exc()
            set_trace()

        try:
            upd = {'repr_inst': v}
            ret = db.iddic.find_one({'name': v})
            if ret:
                try:
                    assert(len(ret['id']) == 1)
                except:
                    print 'Duplicate id: ', v
                    for r in ret['id']:
                        print r
                upd['repr_instid'] = ret['id'][0]

            update(db.boards, {'repr_inst': k}, upd)
        except:
            print_exc()
            set_trace()


def fixdata1():
    # 修正董監事資料錯別字
    print get_funname()
    fi = 'doc/fix_board.yaml'
    dic = {}

    for k, v in yload(fi).iteritems():
        v = adjname.run(v)
        upd = {'repr_inst': v}
        ret = db.iddic.find_one({'name': v})

        if ret:
            try:
                assert(len(ret['id']) == 1)
                upd['repr_instid'] = ret['id'][0]
            except:
                print 'Duplicate id: ', v
                for r in ret['id']:
                    print r

        dic[k] = upd

    li1 = []
    for r in db.cominfo.find():
        chg = 0
        for boss in r['boards']:
            try:
                [boss.__setitem__(k1, v1) for k1, v1
                    in dic[boss['repr_inst']].iteritems()]
                chg = 1
            except:
                """"""
        if chg:
            li1.append(r)
    map(db.cominfo.save, li1)


def get_rawinfo(id):
    # 取得特定統編的原始資料
    fi = [f for f in fis if f[0] == id[0]]

    for r in readg(fi[0]):
        if r[0] == id:
            return r


@deprecated
def defnamedic():
    iddic = {}
    namedic = {}

    def chkname(iddic, name):
        if name not in iddic:
            iddic[name] = []

    dic1 = {'type': {'$in': ['baseinfo', 'fbranchinfo', 'fagentinfo']},
            'status': {'$regex': u'核准'}}
    coldic = {'name': 1, 'id': 1, 'status': 1, 'name1': 1}

    for r in db.cominfo.find(dic1, coldic):
        namedic[r['id']] = r['name']
        chkname(iddic, r['name'])
        iddic[r['name']].append((r['id'], 0))
        if 'name1' in r:
            chkname(iddic, r['name1'])
            iddic[r['name1']].append((r['id'], 1))

    print len(iddic), len(namedic)
    return iddic, namedic


if __name__ == '__main__':
    """"""
    ids = [u'75370905', u'16095002', u'73251209', u'75370601']
    # refresh()
