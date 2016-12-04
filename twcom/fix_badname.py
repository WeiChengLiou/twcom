#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
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


def fun(key, name):
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
                fun(key, x)
            else:
                for x1 in x:
                    fun(key, x1)
    elif isinstance(name, basestring):
        fun(key, name)


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
s = df1.name.value_counts().sort_values()
dbls = s[s > 1].index.get_level_values(0)
dbl_id = (
    id_name
    [id_name['name'].isin(dbls)]
    ['name']
    .drop_duplicates()
    .tolist()
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
# Fix inst as board name
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


##
