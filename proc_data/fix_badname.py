##
import itertools as it
from functools import reduce
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
    ret = df0.merge(df1.rename(columns={col1: 'fix'}), how='left')
    ret['fix'] = ret['fix'].fillna(ret[col])
    assert ret.shape[0] == df0.shape[0]
    return (
        ret.drop(col, axis=1)
        .rename(columns={'fix': col})
        [cols]
    )


##
# Retrieve bad name list

# Construct (id, name) pair dictionary
fixwords = yload('doc/fixword.yaml')
bads = yload('doc/badstatus.yaml')
namecol = ['公司名稱']
id_name = []

ret = db.raw.find(
    {},
    {'_id': 0, 'id': 1,
     '公司名稱': 1,
     '公司狀況': 1,
     '公司狀況文號': 1,
     }
)


def fun(id_name, key, name):
    # fix name and added

    def f_fixword(x, ys):
        return x.replace(ys[0], ys[1])

    name1 = reduce(f_fixword, fixwords.items(), name).strip()

    if name1 and (not name1.isdigit()) and (len(name1) > 3):
        id_name.append(key + (name1,))


for r in ret:
    for col in namecol:
        name = r.get(col)
        if name:
            break
    if not name:
        continue

    word = r.get('公司狀況文號', None)
    status = r['公司狀況']
    id = r['id']
    key = (id, status, word)

    if isinstance(name, list):
        for x in name:
            if isinstance(x, str):
                fun(id_name, key, x)
            else:
                for x1 in x:
                    fun(id_name, key, x1)
    elif isinstance(name, str):
        fun(id_name, key, name)


id_name = (
    pd.DataFrame(id_name, columns=['id', 'status', 'word', 'name'])
)
id_name['source'] = 'cominfo'
id_name = id_name.drop_duplicates()


##
# Edit skip company name list
df1 = id_name[~id_name.status.isin(bads)]
s = chkdist(df1['name'])
skips = [
    '（同名）',
    '（無統蝙）',
    '（無統編）',
    '（股份有限公司）',
    '（因偽造文書撤銷公司設立登記）',
    '（父子）',
    '（新增統編）',
    '(在臺灣地區公司名稱)',
    '(在大陸地區公司名稱)',
    '株式會社',
    '■■■■■',
    '有限會社',
    '(在台灣地區公司名稱)',
    '株式會社',
    '株式会社',
    '有限公司',
    '(株)',
    '「工商憑證申請」',
]

rx = re.compile('商$', re.UNICODE)
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
    rx = re.compile('([\(（].*?[\)）])', re.UNICODE)

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
    {'董監事名單': {'$exists': 1}},
    {'_id': 0, '董監事名單': 1, 'id': 1}
)
boards = []
li = set()
for r in ret:
    for dic in r['董監事名單']:
        dic['id'] = r['id']
        key = dic['id'], dic['姓名'], dic['序號']
        if key not in li:
            boards.append(dic)
            li.add(key)
boards = pd.DataFrame(boards)
boards['職稱'] = (
    boards['職稱']
    .str.replace('([\r\n\t]|null)', '')
    .str.strip()
)
boards['姓名'] = (
    boards['姓名']
    .str.replace('[\r\n\t\u3000]', '')
    .str.strip()
)


##
# Fix inst id/name
x0 = boards.loc[boards['所代表法人'] != "", '所代表法人']
inst = (
    x0.astype(str)
    .str.extract('\[\'?(?P<instid>\d+)\'?, \'?(?P<inst>.*)\'?\]', expand=True)
)
inst['inst'] = inst['inst'].str.replace("'", '')
inst['instid'] = inst['instid'].str.replace('00000000', '0')
boards = boards.join(inst[['instid', 'inst']])

##
# Check non-exists instid
df_noid = (
    inst
    .loc[inst.instid != '0']
    .rename(columns={'instid': 'id'})
    .merge(id_name[['id', 'name']], how='left')
    .pipe(lambda x1: x1.loc[x1['name'].isnull()])
    .drop('name', axis=1)
    .rename(columns={'id': 'instid'})
)
df_noid['inst'] = (
    df_noid['inst']
    .str.replace('(股份|有限|公司)', '')
)
df_noid = df_noid.merge(
    id_name
    .name.str.replace('(股份|有限|公司)', '')
    .rename('inst')
    .to_frame()
    .assign(c=1),
    how='left'
)

if df_noid.c.isnull().sum() > 0:
    print('WARNING: Get unknown instid with unknown inst names')
    print(df_noid)


##
# Check wrong inst name
fixdic = yload('doc/fix_board.yaml')

rename_dic = {}
df2 = inst[~inst['inst'].apply(chk_board)].inst.drop_duplicates()
for x in df2:
    if x not in fixdic:
        rename_dic[x] = ''
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
boards.loc[boards['inst'] == '', 'inst'] = np.nan


##
# Remove bad board name
ret = boards[~boards['姓名'].apply(chk_board)]
boards.loc[ret.index, '姓名'] = ''


# Remove instid whose inst has been removed
boards.loc[
    boards['inst'].isnull() &
    (boards['instid'] == '0'),
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
ret = boards[boards['inst'].notnull()]
ret = ret[ret['instid'].isnull()]
assert len(ret) == 0


##
# Parse special name
def parse_name(name):
    # DO NOT parse English name to reduce subsequent name-match error
    def len_filter(s):
        qry = re.search('[a-zA-Z\d\-\.\(\)\,]+', s)
        if qry:
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
        name = name.replace(li[-1], '').replace('()', '')

    li.extend(name.split(','))
    li = [x.strip() for x in li]
    return set(filter(len_filter, li))


##
# Add resposible person
namecol = (
    ('代表人姓名', '代表人'),
    ('負責人姓名', '負責人'),
    ('訴訟及非訴訟代理人姓名', '訴訟及非訴訟代理人'),
)
titles = [x[1] for x in namecol]

# Raw data don't have these titles in 董監事名單.
# But we drop data by these title when we have to re-run this block.
boards = boards[~boards['職稱'].isin(titles)]

for c, title in namecol:
    print(c)
    ret = db.raw.find(
        {c: {'$exists': 1, '$ne': ''}},
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
            .rename('姓名')
            .reset_index()
            .drop_duplicates()
        )

    x1 = (
        li
        [~li['list']]
        .rename(columns={
            c: '姓名',
        })
        .drop('list', axis=1)
    )

    li = pd.concat([x0, x1])
    li['職稱'] = title
    li = li[li['姓名'].apply(chk_board)]

    # Special case in name
    s = li[li['姓名'].apply(
        lambda x: (re.search('[,\(\)]', x) is not None)
    )]
    if len(s) > 0:
        s1 = (
            s
            .groupby(['姓名'])
            ['姓名']
            .apply(lambda x: parse_name(x.values[0]))
        )
        s2 = []
        for k, vs in s1.items():
            for v in vs:
                s2.append((k, v))
        s2 = pd.DataFrame(s2, columns=['姓名', 'name'])
        li = li.merge(s2, how='left')
        idx = li.loc[li.name.notnull()].index
        li.loc[idx, '姓名'] = li.loc[idx, 'name']
        li = li.drop('name', axis=1).drop_duplicates()

    ret = li.merge(
        boards
        [['id', '姓名']]
        .assign(flag=1),
        how='left'
    )
    ret = (
        ret.loc[
            (ret['flag'].isnull()) &
            (ret['姓名'].apply(chk_board))
        ]
        .drop('flag', axis=1)
    )

    boards = pd.concat([ret, boards])


##
# Fix instid by inst
ret = (
    boards.loc[
        (boards['inst'].notnull()) &
        (boards['instid'] == '0'),
        ['id', 'inst']
    ]
    .drop_duplicates()
)

name_cnt = (
    id_name
    .merge(
        ret[['inst']]
        .rename(columns={'inst': 'name'})
        .drop_duplicates()
    )
    .name.value_counts()
)

# Fix one-to-one match
df1 = (
    id_name
    .loc[
        id_name.name.isin(name_cnt[name_cnt == 1].index.tolist()),
        ['id', 'name']
    ]
)
ret = (
    ret
    .merge(
        df1.rename(columns={
            'id': 'fix',
            'name': 'inst',
        }),
        how='left')
    .dropna()
)
boards = update(boards, ret, 'instid', 'fix')

# Skip multiple-to-one match until 法人代表 list constructed

##
# Assign unique id to no-id-inst by (id, inst)
with open('doc/uniq_org_filter.txt', 'r') as f:
    uniqs = f.read().strip().replace('\n', '|')
    rx = re.compile('({})'.format(uniqs))

ret = (
    boards.loc[
        (boards['inst'].notnull()) &
        (boards['instid'] == '0'),
        ['id', 'inst']
    ]
    .drop_duplicates()
    .sort_values(['inst', 'id'])
)
ret['fix'] = [('T%07d' % x) for x in range(len(ret))]

idx = ret[ret.inst.str.extract(rx, expand=False).notnull()].index
for inst, df_ in ret.loc[idx].groupby('inst'):
    fix = df_.fix.min()
    ret.loc[df_.index, 'fix'] = fix
boards = update(boards, ret, 'instid', 'fix')


##
# Build up 法人代表 list
ret = (
    boards.loc[
        boards['inst'].notnull() & (boards['姓名'] != ''),
        ['instid', '姓名']
    ]
    .drop_duplicates()
    .rename(columns={'instid': 'id'})
    .merge(
        boards
        [['id', '姓名']]
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

ret_boards = ret.drop_duplicates()
ret_boards['職稱'] = '法人代表'
boards = pd.concat([boards, ret_boards])


##
# Fix instid by inst (step 2)
# Sync same `inst` but different `instid` by boards comparison
df1 = (
    boards
    [['inst', 'instid']]
    .drop_duplicates()
    .dropna()
)
cnt = df1.inst.value_counts()
df1 = (
    df1[df1.inst.isin(cnt[cnt > 1].index)]
    .sort_values(['inst', 'instid'])
)
board_ids = boards.merge(df1.instid.to_frame()).id.drop_duplicates()
df2 = boards[boards.id.isin(board_ids)]

id_map = (
    df1[['instid']]
    .assign(fix=lambda x: x['instid'])
    .set_index('instid')
    .fix
)

for inst, df_ in df1.groupby('inst'):
    df3 = (
        df2
        .loc[df2.instid.isin(df_.instid), ['id', 'instid']]
        .drop_duplicates()
    )
    df4 = (
        df2.loc[df2.id.isin(df3.id)]
        .drop('instid', axis=1)
        .merge(df3)
        [['姓名', 'instid']]
        .drop_duplicates()
        .set_index(['姓名', 'instid'])
        .assign(C=1.)
        .C
        .unstack()
        .fillna(0)
        .astype(int)
    )
    grps = []
    grpdic = {}
    for id1, id2 in it.combinations(df4.columns, 2):
        cnt = (df4[[id1, id2]].sum(axis=1) == 2).sum()
        if cnt > 1:
            x = [id1, id2]
            fix = id_map.loc[x].min()
            idgrp = id_map.loc[x].drop_duplicates().tolist()
            id_map.loc[id_map.isin(idgrp)] = fix

boards = update(boards, id_map.reset_index(), 'instid', 'fix')


##
# update id_name after instid drop duplicates
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


##
# Clean up duplicate boards by (id, 姓名)
# DEPRECATED: Since the seats will be reduced.

# skip_rolls = ('代表人', '負責人', '訴訟及非訴訟代理人')
# ret = boards[(~boards['職稱'].isin(skip_rolls))]
# cnt = (
#     ret[ret['姓名'] != '']
#     .groupby(['id', '姓名'])
#     ['序號'].count()
#     .rename('cnt')
# )
# cnt = cnt[cnt > 1].reset_index().drop('cnt', axis=1)
# df2 = ret.merge(cnt).groupby(['id', '姓名']).first().reset_index()
# ret = ret.merge(cnt, how='left', indicator=True)
# ret = pd.concat([
#     ret[ret['_merge'] == 'left_only'].drop('_merge', axis=1),
#     df2
# ]).sort_values(['id', '序號'])
# boards = ret


##
# Export organization list
ret = boards[boards['inst'].notnull()]
ret = ret[ret['instid'].apply(lambda x: 'T' in x)]
orgs = chkdist(ret['inst'])
orgs.to_csv('doc/org_list.csv', encoding='utf8', sep='\t')


##
# Write data into database
# table: cominfo1
coll = db.cominfo1
coll.drop()
coll.insert_many(id_name.to_dict('record'))
print('%s inserted %s items' % (coll.name, coll.count()))

##
# table: boards1
coll = db.boards1
coll.drop()
coll.insert_many(boards.to_dict('record'))
print('%s inserted %s items' % (coll.name, coll.count()))

##
