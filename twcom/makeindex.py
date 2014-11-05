#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import pymongo
import time
from work import *
from utils import *

path = 'files'
path = 'TW Company Download/files'

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


# 每個 json item 可分為以下類別
# 獨資企業： indinfo
# 本土分公司： branchinfo
# 外資在臺註冊分公司： fbranchinfo
# 外資在臺註冊辦事處：fagentinfo
# 本土或外資在台註冊企業： baseinfo


class ComItem(object):
    def __init__(self, kv):
        self.id = kv[0]

        # 初次對 json 分類為不同類別
        if u'組織類型' in kv[1]:
            self.tbl = 'indinfo'
            self.read(kv, filedic['indinfo'])
        elif u'分公司名稱' in kv[1]:
            self.tbl = 'branchinfo'
            self.read(kv, filedic['branchinfo'])
        elif u'在中華民國境內營運資金' in kv[1]:
            self.tbl = 'fbranchinfo'
            self.read(kv, filedic['fbranchinfo'])
        elif u'在台灣地區營業所用' in kv[1]:
            self.tbl = 'fbranchinfo'
            self.read(kv, filedic['fbranchinfo'])
        elif u'辦事處所在地' in kv[1]:
            self.tbl = 'fagentinfo'
            self.read(kv, filedic['fagentinfo'])
        else:
            self.tbl = 'baseinfo'
            self.read(kv, filedic['baseinfo'])

    def read(self, kv, coldic):
        for k, v in kv[1].iteritems():
            key = coldic.get(k)
            if not k:
                # 遇到異常欄位就要先註記再即時 debug
                if k not in errcol.keys():
                    # 列出異常欄位名稱與公司統編
                    print u'Unknown Column:', kv[0], self.tbl, k
                    errcol[k] = (self.id, v)
                if self.id not in errid.has_key:
                    errid[self.id] = (self.id, kv[1])
                pdb.set_trace()
            else:
                self.process(key, v)

    def process(self, key, v):
        if key == 'name' and isinstance(v, list):
            if isinstance(v[1], list):
                self.name1 = v[1][0]
                self.name = v[0][0]
            else:
                self.name1 = v[1]
                self.name = v[0]
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
                val = int(v1) if v1 != '' else 0
                self.__dict__[key] = val
                return
            except:
                print_exc()
                set_trace()
        elif key == 'status':
            v = replaces(v, (u' ', u'　', 'null'))

        self.__dict__[key] = v

    def insinfo(self):
        # 輸入公司基本資料，每間公司有自己的類別
        items = {'type': self.tbl}
        for col in tblcol[self.tbl]:
            if not hasattr(self, col):
                continue
            try:
                items[col] = self.__dict__[col]
            except:
                print_exc()
                set_trace()

        if hasattr(self, 'boardcnt'):
            # 公司基本資料新增董監事人數
            items['boardcnt'] = self.boardcnt

        try:
            cn.cominfo.insert(items)
        except:
            print_exc()
            set_trace()

    def insboards(self):
        # 輸入董監事資料
        if 'boards' not in self.__dict__:
            return
        boardic = filedic['boards']

        # 各公司董監事名單使用 set 儲存，因為可能有重複
        sets = set()
        cnt = 0
        for boss in self.boards:
            vs = {'id': self.id}
            for col, v in boss.iteritems():
                if col == u'所代表法人':
                    if isinstance(v, list):
                        vs['repr_inst'] = v[1]
                        vs['repr_instid'] = v[0]
                    else:
                        vs['repr_inst'] = v
                        vs['repr_instid'] = '0'
                else:
                    vs[boardic[col]] = v

            kvs = tuple([vs[k] for k in (
                'id', 'name', 'repr_inst', 'repr_instid')])
            if kvs in sets:
                # 另外紀錄董監事名單裡重複的名字
                with open('boards_dbl.csv', 'a') as f:
                    f.write(u','.join(map(unicode, kvs)).encode('utf8'))
                    f.write(u'\n'.encode('utf8'))
            else:
                sets.add(kvs)

            try:
                cn.boards.insert(vs)
                cnt += 1
            except:
                print_exc()
                set_trace()

        self.boardcnt = cnt


def getfile(dst):
    print "Please goto 'http://gcis.nat.g0v.tw/'"


def instbl(kv):
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
    f1 = lambda kv: map(lambda fun: fun(kv), args)
    map(f1, kvs)


def refresh():
    # 新增、處理資料庫
    global cn

    cn = init()
    cn.cominfo.drop()
    cn.cominfo.ensure_index([('id', 1), ('name', 1)], unique=True)
    cn.boards.drop()
    cn.boards.ensure_index(
        [
            ('id', 1), ('name', 1),
            ('repr_instid', 1), ('repr_inst', 1),
            ('title', 1), ('equity', 1)],
        unique=True)
    f = open('boards_dbl.csv', 'w')
    f.close()
    runjobs(instbl)

    fixing()


def fixing():
    # Preprocessing

    # 新增公司名稱與統編對照（僅留下尚在經營公司）
    ins_iddic()
    # 修正董監事資料錯別字
    fixdata()
    # 新增董監事名單錯別字至名稱對照
    fixrepr()

    for i in range(20):
        # 修正董監事名單（母函數）
        # 設置迴圈是因為有可能需要檢查兩次以上
        cnt = fixboards()
        if cnt == 0:
            break
        time.sleep(10)

    if cnt == 0:
        ins_comivsts()
        rm_badcomivst()
    else:
        logger.info('Ignore comivst insert process')

    # if nations:
    #     cPickle.dump((nations,), open('dics.dat', 'wb'), True)


def defnation():
    # 找出國家別
    nations = set()
    tbls = ('fbranchinfo', 'fagentinfo')
    for r in cn.cominfo.find({'type': {'$in': tbls},
                              'name': {'$regex': u'商'}},
                             ['name']):
        name = r['name']
        i = name.index(u'商')
        nations.add(name[:(i+1)])
    nations = list(nations)
    print len(nations)
    return nations


def ins_iddic():
    # 新增公司名稱與統編對照（僅留下尚在經營公司）
    cn.iddic.drop()
    cn.iddic.ensure_index([('name', 1)])

    def ins_update(name, id):
        ret = cn.iddic.find_one({'name': name})
        if ret:
            ret['id'].append(id)
            cn.iddic.save(ret)
        else:
            cn.iddic.insert({'name': name, 'id': [id]})

    dic1 = {'type': {'$in': ['baseinfo', 'fbranchinfo', 'fagentinfo']},
            'status': {'$regex': u'核准'}}
    coldic = {'name': 1, 'id': 1, 'status': 1, 'name1': 1}

    for r in cn.cominfo.find(dic1, coldic):
        ins_update(r['name'], r['id'])
        if 'name1' in r:
            ins_update(r['name1'], r['id'])


def ins_comivsts():
    # 新增公司連結母函數
    cn.comivst.drop()
    cn.comivst.ensure_index([
        ('src', 1), ('dst', 1)])
    comall = cn.boards.distinct('id')
    map(ins_comivst, chunk(comall, 200))


def ins_comivst(coms):
    # 新增公司連結子函數
    seatdic = {
        k['id']: k['boardcnt'] for k in cn.cominfo.find(
            {'boardcnt': {'$gt': 0},
             'id': {'$in': coms}},
            ['id', 'boardcnt'])}
    query = {'id': {'$in': coms},
             'repr_inst': {'$ne': ''}}

    chkdic = defaultdict(set)
    dic = groupdic(cn.boards.find(query,
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
            'seatratio': cntr}
        cn.comivst.insert(items)

        chkdic[srcid].update([r['name'] for r in rs])

    fill_hideboss(chkdic)


def fill_hideboss(chkdic):
    # 新增各公司非董監事的法人代表
    for r in cn.boards.find({'id': {'$in': chkdic.keys()}}):
        if r['id'] in chkdic and r['name'] in chkdic[r['id']]:
            chkdic[r['id']].remove(r['name'])

    for id, names in chkdic.iteritems():
        for name in names:
            dic = {'id': id,
                   'name': name,
                   'title': u'法人代表',
                   'repr_inst': u'',
                   'repr_instid': 0,
                   'target': id,
                   'equity': 0}
            cn.boards.save(dic)


def rm_badcomivst():
    # 移除公司連結裡非營業中的企業。
    try:
        bads = list(badstatus(cn))

        coms = set(cn.comivst.distinct('src'))
        coms.update(cn.comivst.distinct('dst'))
        dic = {'id': {'$in': list(coms)},
               'status': {'$in': list(bads)}}
        badcoms = cn.cominfo.find(dic).distinct('id')
        print 'badcoms count: {0}'.format(len(badcoms))
        cn.comivst.remove({'src': {'$in': badcoms}})
        cn.comivst.remove({'dst': {'$in': badcoms}})
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
            except pymongo.errors.DuplicateKeyError:
                if errfun:
                    errfun(x)
                else:
                    return x


# Clean Data
def fixboards():
    # 修正董監事名單（母函數）
    condition = {'repr_inst': {'$ne': ''}}
    reprids = cn.boards.find(condition).distinct('repr_inst')
    step = 200
    totcnt = len(reprids) / 200 + 1
    logger.info('fixboards:  Total Count - {0}'.format(len(reprids)))
    toterr = 0
    for i, x in enumerate(chunk(reprids, step)):
        logger.info('fixboards: {0} / {1}'.format(i, totcnt))
        errcnt, li = fixboard(x)
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
    for r in cn.boards.find({'repr_inst': {'$in': names}}):
        key = r['repr_instid'], r['repr_inst']
        dic[key].append(r)

    for key, items in dic.iteritems():
        id, name = key

        try:
            if id and (len(id) == 8):
                # 檢查法人代表裡公司名稱是否與基本資料相符
                ret = cn.cominfo.find_one({'id': id}, ['id', 'name'])
                if ret and name != ret['name']:
                    [upd_item(cn.boards,
                              item,
                              {'repr_inst': ret['name']})
                        for item in items]
                    li.append((id, name, ret['name']))
                    cnt += 1
            else:
                # 從法人代表公司名稱反查法人代表統編
                ret = cn.iddic.find_one({'name': name})
                if ret is None:
                    continue

                ids = ret['id']
                if len(ids) == 1:
                    [upd_item(cn.boards,
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


def fixdata():
    # 修正董監事資料錯別字
    fi = 'fix_board.txt'
    with open(fi) as f:
        for li in f:
            k, v = li[:-1].decode('utf8').split('\t')
            print k, v
            try:
                assert(isinstance(k, unicode))
                assert(isinstance(v, unicode))
            except:
                pdb.set_trace()

            try:
                upd = {'repr_inst': v}
                ret = cn.iddic.find_one({'name': v})
                if ret:
                    try:
                        assert(len(ret['id']) == 1)
                    except:
                        print 'Duplicate id: ', v
                        for r in ret['id']:
                            print r
                    upd['repr_instid'] = ret['id'][0]

                update_col(cn.boards, {'repr_inst': k}, upd)
            except:
                print_exc()
                set_trace()


def fixrepr():
    # 新增董監事名單錯別字至名稱對照
    def chgfun(name):
        for q in cn.iddic.find({'name': name}):
            if len(q['id']) > 1:
                sprint([name, q['id']])
            else:
                for r in cn.boards.find({'name': name}):
                    r['repr_instid'] = q['id'][0]
                    r['repr_inst'] = name
                    cn.boards.save(r)
            chgfun.cnt += 1
    chgfun.cnt = 0

    for name in it.ifilter(lambda name: len(name) > 3,
                           cn.boards.distinct('name')):
        if u'證卷' in name:
            name = name.replace(u'證卷', u'證券')
        chgfun(name)

        if u'台灣' in name:
            chgfun(name.replace(u'台灣', u'臺灣'))
        elif u'臺灣' in name:
            chgfun(name.replace(u'臺灣', u'台灣'))
    logger.info('{0}: {1}'.format(get_funname(), chgfun.cnt))


def get_rawinfo(id):
    # 取得特定統編的原始資料
    fi = [f for f in fis if f[0] == id[0]]

    for r in readg(fi[0]):
        if r[0] == id:
            return r


# unused functions
def defnamedic():
    iddic = {}
    namedic = {}

    def chkname(iddic, name):
        if name not in iddic:
            iddic[name] = []

    dic1 = {'type': {'$in': ['baseinfo', 'fbranchinfo', 'fagentinfo']},
            'status': {'$regex': u'核准'}}
    coldic = {'name': 1, 'id': 1, 'status': 1, 'name1': 1}

    for r in cn.cominfo.find(dic1, coldic):
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
    #refresh()

