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
    assert df1[col1].isnull().sum() == 0
    cols = df0.columns
    if col1 is None:
        col1 = col
    ret = (
        df0
        .merge(df1.rename(columns={col1: 'fix'}), how='left')
    )
    ret['fix'] = ret['fix'].fillna(ret[col])
    assert ret.shape[0] == df0.shape[0]
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
    if len(cnt) > 0:
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
    else:
        return None


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

    if name1 and (not name1.isdigit()) and (len(name1) > 3):
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
boards = pd.DataFrame(boards)


##
# Fix inst id/name
inst = (
    boards[boards[u'所代表法人'] != ""][u'所代表法人']
    .to_frame()
)
inst['instid'], inst['inst'] = zip(*inst[u'所代表法人'].tolist())
boards = boards.join(inst[['instid', 'inst']])

# Check non exists instid
ids = inst['instid'].drop_duplicates()
id2 = ids[~ids.isin(id_name['id'])]
id2 = id2[id2 != 0]
names = inst.ix[id2.index, 'inst'].apply(
    lambda x: x.replace(u'股份有限公司', u''))
rx = re.compile(u'|'.join(names), re.UNICODE)
df_noid = id_name[id_name.name.apply(lambda x: rx.search(x) is not None)]

if len(df_noid) > 0:
    raise Exception('Get exists name but unknown id', df_noid)


##
# Check wrong inst name
fixdic = yload('doc/fix_board.yaml')

rename_dic = {}
df2 = inst[~inst['inst'].apply(chk_board)].inst.drop_duplicates()
for x in df2:
    if x not in fixdic:
        rename_dic[x] = u''
fixdic.update(rename_dic)

fixdic = (
    pd.Series(fixdic)
    .rename('fix')
    .to_frame()
    .reset_index()
    .rename(columns={
        'index': 'name',
    })
)
# show(fixdic)


##
# Fix bad inst name
boards = update(
    boards,
    fixdic.rename(columns={'name': 'inst'}),
    'inst',
    'fix'
)
boards.ix[boards['inst'] == u'', 'inst'] = np.nan


##
# Remove bad board name
ret = boards[~boards[u'姓名'].apply(chk_board)]
boards.ix[ret.index, u'姓名'] = u''


# Remove instid which remove inst too
boards.ix[
    boards['inst'].isnull() &
    (boards['instid'] == 0),
    'instid'] = np.nan

##
# Fix wrong inst based on instid
iddic = (
    id_name
    .groupby('id')
    ['name']
    .first()
    .reset_index()
)
boards = update(
    boards,
    iddic.rename(columns={'id': 'instid'}),
    'inst', 'name'
)


##
# Find inst with empty instid
ret = boards[
    (boards['inst'].notnull()) &
    (boards['inst'] != u'')
]

ret = ret[ret['instid'].isnull()]
assert len(ret) == 0


##
# Parse special name
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
titles = [x[1] for x in namecol]
boards = boards[~boards[u'職稱'].isin(titles)]

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
        # Deal with name list case
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
# Assign unique id to no-id-inst
ret = (
    boards
    [
        (boards['inst'].notnull()) &
        (boards['instid'] == 0)
    ]
    [['id', 'inst']]
    .drop_duplicates()
)
ret['fix'] = [('T%07d' % x) for x in range(len(ret))]
boards = update(boards, ret, 'instid', 'fix')


##
# Build up inst representatives list
ret = (
    boards.ix[boards['inst'].notnull(), ['instid', u'姓名']]
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

ret_boards = ret.drop_duplicates()
ret_boards[u'職稱'] = u'法人代表'
boards = pd.concat([boards, ret_boards])


##
# Brute force same company by name
id_name['keyno'] = id_name['id']
cnt = 0
while 1:
    print 'Big loop'
    id_fix = (
        grp_unify(id_name.set_index(['name'])['keyno'])
        .drop('name', axis=1)
        .drop_duplicates()
    )
    df1_fix = grp_unify(
        id_fix
        .set_index(['keyno'])
        ['keynofix']
    )
    if df1_fix is not None:
        print 'sub update'
        id_fix = (
            update(id_fix, df1_fix, 'keynofix', 'keynofixfix')
            .drop_duplicates()
            )
        id_name = update(id_name, id_fix, 'keyno', 'keynofix')
    cnt1 = id_name['keyno'].drop_duplicates().count()
    if cnt1 == cnt:
        break
    else:
        cnt = cnt1


##
# Fix instid with inst
idmap = (
    id_name[
        (id_name['source'] == 'org') &
        (id_name['id'] != id_name['keyno'])
    ]
    [['id', 'keyno']]
)
boards = update(boards, idmap.rename(columns={'id': 'instid'}),
                'instid', 'keyno')
id_name = id_name.drop(idmap.index)


##
# Fix inst as board name
com_name = (
    id_name['name']
    .rename(u'姓名')
    .drop_duplicates()
    .to_frame()
)
ret = boards.merge(com_name)
ret = (
    ret[ret['instid'].isnull()]
    [['id', u'姓名']]
)
ret1 = (
    id_name
    [id_name['name'].isin(ret[u'姓名'])]
    .groupby('name')
    .keyno
    .first()
    .reset_index()
    .rename(columns={
        'name': 'inst',
        'keyno': 'instid'
    })
)
ret1[u'姓名'] = ret1['inst']
ret = ret.merge(ret1)
if len(ret) > 0:
    boards.set_index(['id', u'姓名'])
    ret.set_index((['id', u'姓名']))
    boards.update(ret)
    boards.reset_index()


##
# Export organization list
ret = boards[boards['inst'].notnull()]
ret = ret[ret['instid'].apply(lambda x: 'T' in x)]
orgs = chkdist(ret['inst'])
orgs.to_csv('doc/org_list.csv', encoding='utf8', sep='\t')


##
