#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
import itertools as it
import numpy as np
import re
import pandas as pd
from twcom.utils import db, chk_board
from twcom.work import show, yload
show(pd.__version__)


##
# Check distribution of series
def chkdist(s):
    name = s.name
    ret1 = (
        s
        .value_counts()
        .rename('cnt')
        .reset_index()
        .rename(columns={
            'index': name,
        })
    )
    ret1['len'] = ret1[name].apply(len)
    return ret1


##
def update(df0, df1, col, col1=None):
    # Update column from another dataframe
    cols = df0.columns
    if col1 is None:
        col1 = col
    ret = (
        df0
        .merge(df1.rename(columns={col1: 'fix'}), how='left')
    )
    ret['fix'] = ret['fix'].fillna(ret[col])
    return (
        ret
        .drop(col, axis=1)
        .rename(columns={'fix': col})
        [cols]
    )


##
def grp_unify(df):
    """ Group select and offer fix id """
    lvls = range(df.index.nlevels)
    col = df.name
    colfix = col + 'fix'
    cnt = (
        df.groupby(level=lvls)
        .count()
    )
    cnt = cnt[cnt > 1]
    df1 = (
        df.ix[cnt.index]
        .copy()
        .to_frame()
    )
    df2 = (
        df1
        .groupby(level=lvls)
        [col]
        .min()
        .rename(colfix)
    )
    df3 = (
        df1
        .reset_index()
        .merge(df2.reset_index())
        .drop_duplicates()
    )
    return df3


##
# Retrieve bad name list

# Construct (id, name) pair dictionary
fixwords = yload('doc/fixword.yaml')
bads = yload('doc/badstatus.yaml')
namecol = [u'公司名稱']
id_name = []

ret = db.raw.find(
    {},
    {'_id': 0, 'id': 1,
     u'公司名稱': 1,
     u'公司狀況': 1,
     u'公司狀況文號': 1,
     }
)


def fun(id_name, key, name):
    # fix name and added

    def f_fixword(x, ys):
        return x.replace(ys[0], ys[1])

    name1 = reduce(f_fixword, fixwords, name).strip()

    if name1:
        id_name.append(key + (name1,))


for r in ret:
    for col in namecol:
        name = r.get(col)
        if name:
            break
    if not name:
        continue

    word = r.get(u'公司狀況文號', None)
    status = r[u'公司狀況']
    id = r['id']
    key = (id, status, word)

    if isinstance(name, list):
        for x in name:
            if isinstance(x, basestring):
                fun(id_name, key, x)
            else:
                for x1 in x:
                    fun(id_name, key, x1)
    elif isinstance(name, basestring):
        fun(id_name, key, name)


id_name = (
    pd.DataFrame(id_name, columns=['id', 'status', 'word', 'name'])
)
id_name['source'] = 'cominfo'


##
# Edit skip company name list
df1 = id_name[~id_name.status.isin(bads)]
s = chkdist(df1['name'])
skips = [
    u'（同名）',
    u'（無統蝙）',
    u'（無統編）',
    u'（股份有限公司）',
    u'（因偽造文書撤銷公司設立登記）',
    u'（父子）',
    u'（新增統編）',
    u'(在臺灣地區公司名稱)',
    u'(在大陸地區公司名稱)',
    u'株式會社',
    u'■■■■■',
    u'有限會社',
    u'(在台灣地區公司名稱)',
    u'株式會社',
    u'株式会社',
    u'有限公司',
    u'(株)',
]

rx = re.compile(u'商$', re.UNICODE)
ret = s[
    (s['name'].apply(lambda x: rx.search(x) is not None)) &
    (s['len'] < 10)
]
skips.extend(ret['name'])

rx = re.compile('^[\d\w]+$')
ret = s[
    (s['name'].apply(lambda x: rx.search(x) is not None)) &
    (s['len'] < 5)
]
skips.extend(ret['name'])
id_name = id_name[~id_name['name'].isin(skips)]


##
df1 = id_name[~id_name.status.isin(bads)]
# df2 = id_name[~id_name['name'].apply(chk_board)]
s = chkdist(df1['name'])

dbls = s[s.cnt > 1]['name']
dbl_id = (
    id_name
    [id_name['name'].isin(dbls)]
    ['name']
    .drop_duplicates()
    .tolist()
)
dbldic = (
    id_name
    [id_name.name.isin(dbls)]
    .groupby('name')
    ['id']
    .apply(lambda x: list(x))
    .to_dict()
)


##
# Study bad company name
def study_badname():
    li = []
    rx = re.compile(u'([\(（].*?[\)）])', re.UNICODE)

    for i, r in id_name.iterrows():
        for x in rx.findall(r['name']):
            li.append((r['id'], r['status'], r['word'], x))

    li = pd.DataFrame(li, columns=['id', 'status', 'word', 'name'])

    li1 = li['name'].value_counts().sort_values()
    li1.to_csv('temp.csv', sep='\t', encoding='utf8')
    return li


##
# Get board full list
ret = db.raw.find(
    {u'董監事名單': {'$exists': 1}},
    {'_id': 0, u'董監事名單': 1, 'id': 1}
)
boards = []
for r in ret:
    for dic in r[u'董監事名單']:
        dic['id'] = r['id']
        boards.append(dic)
boards = pd.DataFrame(boards)


##
# Fix inst id/name
inst = (
    boards[boards[u'所代表法人'] != ""][u'所代表法人']
    .to_frame()
)
inst['instid'], inst['inst'] = zip(*inst[u'所代表法人'].tolist())
boards = boards.join(inst[['instid', 'inst']])

# Check non exists id
ids = inst['instid'].drop_duplicates()
id2 = ids[~ids.isin(id_name['id'])]
id2 = id2[id2 != 0]
names = inst.ix[id2.index, 'inst'].apply(
    lambda x: x.replace(u'股份有限公司', u''))
rx = re.compile(u'|'.join(names), re.UNICODE)
df2 = id_name[id_name.name.apply(lambda x: rx.search(x) is not None)]

if len(df2) > 0:
    raise Exception('Get exists name but unknown id', df2)


##
# Check wrong name
fixdic = yload('doc/fix_board.yaml')

rename_dic = {}
df2 = inst[~inst['inst'].apply(chk_board)].inst.drop_duplicates()
for x in df2:
    if x not in fixdic:
        rename_dic[x] = u''
fixdic.update(rename_dic)

fixdic = (
    pd.Series(fixdic)
    .to_frame()
    .reset_index()
    .rename(columns={
        'index': 'name',
        0: 'fix',
    })
)
# show(fixdic)


##
# Remove bad board name
ret = boards[~boards[u'姓名'].apply(chk_board)]
boards.ix[ret.index, u'姓名'] = u''


##
# Fix bad inst name
ret = (
    boards
    .merge(
        fixdic.rename(columns={'name': 'inst'}),
        how='left'
    )
)
ret['fix'] = ret['fix'].fillna(ret['inst'])

# Remove instid which remove inst too
ret.ix[(ret['fix'] == u'') & (ret['instid'] == 0), 'instid'] = np.nan

ret = (
    ret
    .drop('inst', axis=1)
    .rename(columns={'fix': 'inst'})
)
boards = ret


##
iddic = (
    id_name
    .groupby('id')
    ['name']
    .first()
    .reset_index()
)


##
# Fix wrong inst based on instid
ret = (
    boards
    .merge(
        iddic
        .rename(columns={
            'id': 'instid',
        }),
        how='left'
    )
)

idx = ret[(ret['inst'] != ret['name']) & ret['name'].notnull()].index
ret.ix[idx, 'inst'] = ret.ix[idx, 'name']
boards = ret.drop('name', axis=1)


##
# Find inst with empty instid
ret = boards[
    (boards['inst'].notnull()) &
    (boards['inst'] != u'')
]

ret = ret[ret['instid'].isnull()]
assert len(ret) == 0


##
# Solution to deal with one company name with several ids
ret = (
    boards[
        (boards['inst'].isin(dbls))
    ]
)
for i, df_ in ret.iterrows():
    instid, inst = df_['instid'], df_['inst']
    ids = dbldic[inst]

    if instid in ids:
        # matched instid
        continue
    elif instid == 0:
        # Check boards coverage count
        dic = (
            boards
            [boards['id'].isin(ids)]
            .groupby('id')
            .apply(lambda x: set(x[u'姓名']))
        )
        board0 = set(boards[boards['id'] == df_['id']][u'姓名'])
        covers = dic.apply(lambda x: len(x & board0))
        if covers.max() == 0:
            msg = (u'Warning: no match boards - %s' % inst).encode('utf8')
            raise Exception(msg)
        instid1 = covers.argmax()
        boards.ix[df_.name, 'instid'] = instid1


##
"""
處理以法人代表名稱尋找對應 id 的問題時，
會以 id-name 清單作為對照確認。

第一個問題是一個名稱可能有多個 id。
這有部份是來自於公司併購：例如元大期貨。
所以單純的透過比較該 id 是否在已有清單內時，
就會有可能該 id 已解散的問題。
但是像是合併解散的情況，我們無法得知存活公司是誰。
所以還是從簡處理，若已在現有清單內則略過。
若否則比對董監事名單，
但目前尚未處理法人代表的問題，
待日後再行處理。

考慮法人代表的處理方式：

- 先根據現有董監名單初步修正 instid by inst
- 建立法人代表名單，Group by (id, instid, inst)，無統編者一律視為不同單位。
- 根據法人代表與董監名單，判斷哪些相同公司名稱但不同 id 者，應視為相同單位。
- Update instid by inst，according to Step 3.
- 若有更新 instid 者，回到步驟二重新檢查，否則結束。
- 依前項結果檢視公司名稱被當作董監事名稱者。
- 確定 org list，無統編者一律視為不同單位。
"""
##
# Fix instid with inst
ret = (
    boards[
        # (~boards['inst'].isin(dbls)) &
        (boards['inst'].notnull()) &
        (boards['inst'] != u'')
        # (boards['instid'] != 0)
    ]
    [['id', 'instid', 'inst']]
    .drop_duplicates()
)
ret = (
    ret
    .merge(
        id_name
        [id_name['name'].isin(ret['inst'])]
        .groupby('name')
        .apply(lambda x: list(x['id']))
        .rename('uid_list')
        .reset_index()
        .rename(columns={'name': 'inst'}),
        how='left'
    )
    .dropna()
)


##
li = []
errs = []

for i, df_ in ret.iterrows():
    instid, inst = df_['instid'], df_['inst']
    ids = df_['uid_list']

    if (instid in df_['uid_list']):
        continue

    # Check boards coverage count
    dic = (
        boards
        [boards['id'].isin(ids)]
        .groupby('id')
        .apply(lambda x: set(x[u'姓名']))
    )
    if (len(dic) == 0):
        if (instid in df_['uid_list']):
            continue
        msg = 'Empty boards'
        errs.append(df_.to_dict().items() + [('msg', msg)])
        continue

    board0 = set(boards[boards['id'] == df_['id']][u'姓名'])
    covers = dic.apply(lambda x: len(x & board0))
    if (covers.max() == 0):
        if (instid in df_['uid_list']):
            continue
        msg = 'No match boards'
        errs.append(df_.to_dict().items() + [('msg', msg)])
        continue
    instid1 = covers.argmax()
    li.append(
        dict(df_.to_dict().items() + [('fix', instid1)])
    )

li = pd.DataFrame(li).drop('uid_list', axis=1)
errs = pd.DataFrame(map(dict, errs)).drop('uid_list', axis=1)


##
# Assign id to inst
ret = (
    boards
    [
        (boards['inst'].notnull()) &
        (boards['inst'] != u'') &
        (boards['instid'] == 0)
    ]
    [['id', 'inst']]
    .drop_duplicates()
)
ret['fix'] = [('T%07d' % x) for x in range(len(ret))]
ret = (
    boards
    .merge(ret, how='left')
)
idx = ret['fix'].notnull()
ret.ix[idx, 'instid'] = ret.ix[idx, 'fix']
boards = ret.drop('fix', axis=1)


##
# Deal with special name
def parse_name(name):
    def len_filter(s):
        qry = re.search('[\w\d\-\.\(\)\,]+', s)
        if qry:
            if (qry.group() == s):
                if (len(s) < 5):
                    return False
            else:
                return False
        else:
            if len(s) < 2:
                return False
        return True

    rx1 = re.compile('\((.*)\)', re.UNICODE)
    li = []
    qry = rx1.search(name)
    if qry:
        li.extend(qry.groups())
        name = name.replace(li[-1], u'').replace(u'()', u'')

    li.extend(name.split(','))
    li = [x.strip() for x in li]
    return list(it.ifilter(len_filter, li))


##
# Add resposible person
namecol = (
    (u'代表人姓名', u'代表人'),
    (u'負責人姓名', u'負責人'),
    (u'訴訟及非訴訟代理人姓名', u'訴訟及非訴訟代理人'),
)
# for c, title in namecol:
#     boards = boards[boards[u'職稱'] != title]

for c, title in namecol:
    print c
    ret = db.raw.find(
        {c: {'$exists': 1, '$ne': u''}},
        {'_id': 0, 'id': 1, c: 1}
    )
    li = pd.DataFrame(list(ret))

    li['list'] = li[c].apply(
        lambda x: isinstance(x, list)
    )
    x0 = li[li['list']]
    x0 = pd.DataFrame(
        x0[c].tolist(),
        index=x0['id']
    )

    if len(x0) > 0:
        # Deal with name list
        x0 = (
            pd.concat(
                [x0[0], x0[1]]
            )
            .rename(u'姓名')
            .reset_index()
            .drop_duplicates()
        )

    x1 = (
        li
        [~li['list']]
        .rename(columns={
            c: u'姓名',
        })
        .drop('list', axis=1)
    )

    li = pd.concat([x0, x1])
    li[u'職稱'] = title
    li = li[li[u'姓名'].apply(chk_board)]

    # Special case in name
    s = li[li[u'姓名'].apply(
        lambda x: (re.search(u'[,\(\)]', x) is not None)
    )]
    if len(s) > 0:
        s1 = (
            s.set_index('id')
            [u'姓名']
            .apply(parse_name)
        )
        s1 = (
            pd.DataFrame(
                s1.tolist(),
                index=s1.index
            )
            .stack()
            .rename(u'姓名')
        )
        s1.index = s1.index.droplevel(1)
        s1 = (
            s1
            .reset_index()
            .drop_duplicates()
        )
        s1[u'職稱'] = li.iloc[0][u'職稱']

        li = pd.concat([li, s1]).drop_duplicates()

    ret = (
        li
        .merge(
            boards
            [['id', u'姓名']]
            .assign(flag=1),
            how='left'
        )
    )
    ret = ret[
        (ret['flag'].isnull()) &
        (ret[u'姓名'].apply(chk_board))
    ]
    ret = ret.drop('flag', axis=1)

    boards = pd.concat([ret, boards])


##
# Build up inst representatives list
ret = (
    boards
    [(boards['inst'] != u'') & (boards['inst'].notnull())]
    [['instid', u'姓名']]
    .drop_duplicates()
    .rename(columns={'instid': 'id'})
    .merge(
        boards
        [['id', u'姓名']]
        .drop_duplicates()
        .assign(flag=1),
        how='left'
    )
)
ret = (
    ret
    [ret['flag'].isnull()]
    .drop('flag', axis=1)
)

ret_idname = (
    boards
    [['instid', 'inst']]
    .drop_duplicates()
    .dropna()
    .rename(columns={
        'instid': 'id',
        'inst': 'name',
    })
)
ret_idname = ret_idname[~ret_idname['id'].isin(id_name['id'])]
ret_idname['source'] = 'org'
id_name = pd.concat([id_name, ret_idname])

ret_boards = (
    ret
    .drop_duplicates()
)
ret_boards[u'職稱'] = u'法人代表'
boards = pd.concat([boards, ret_boards])


##
# Compare boards and id_name, build same company list
id_name['keyno'] = id_name['id']
ret = (
    id_name.merge(boards, how='left')
)
id_fix = (
    grp_unify(ret.set_index(['name', u'姓名'])['keyno'])
    .drop(u'姓名', axis=1)
)
df1_fix = grp_unify(
    id_fix
    .set_index(['name', 'keyno'])
    ['keynofix']
)
id_fix = (
    update(id_fix, df1_fix, 'keynofix', 'keynofixfix')
    .drop('name', axis=1)
)
df1_fix = grp_unify(
    id_fix
    .set_index(['keyno'])
    ['keynofix']
)
id_fix = update(id_fix, df1_fix, 'keynofix', 'keynofixfix')
id_name = update(id_name, id_fix, 'keyno', 'keynofix')


##
# Fix inst as board name
ret = (
    boards
    [boards[u'姓名'].isin(id_name['name'])]
)


##
# Export instid==0 list
ret = (
    boards[boards['instid'] == 0]
)
orgs = ret['inst'].value_counts().sort_index()
orgs.to_csv('doc/org_list.csv', encoding='utf8', sep='\t')


##
# Generate organization list
orglist = (
    orgs
    .reset_index()
    .drop('inst', axis=1)
    .rename(columns={'index': 'name'})
)
orglist['id'] = orglist['name']
orglist['source'] = 'org'
id_name = pd.concat([id_name, orglist]).reset_index(drop=True)
"""
合併 orglist 時，會有公司名稱跟現有清單重複的問題。
但因為其董監事名單並無重複，
故為保守起見還是列為新公司。
"""


##
