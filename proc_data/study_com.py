##
import numpy as np
import pandas as pd
from twcom.utils import db
from twcom.work import getdf
import seaborn as sns
sns.set_context('poster')
pd.__version__


def unwind(ret):
    return (
        pd.concat([
            ret,
            pd.DataFrame(ret['_id'].tolist())
        ], axis=1)
        .drop('_id', axis=1)
    )


##
# load id_name
id_name = getdf(db.cominfo1.find({}, {'_id': 0}))


##
id_name = id_name.sort_values(['id', 'name'])
id_name['idx'] = id_name['id'].shift(-1)
iddic = id_name[id_name['id'] != id_name['idx']].drop('idx', axis=1)


##
# get ivst count
coll = db.boards1
ret = coll.aggregate([
    {'$match': {'instid': {'$ne': np.nan}}},
    {'$group': {'_id': {'instid': '$instid', 'id': '$id'}}},
    {'$group': {'_id': '$_id.instid', 'cnt': {'$sum': 1}}},
    {'$sort': {'cnt': -1}},
    {'$limit': 1000}
])
ret = (
    getdf(ret)
    .rename(columns={'_id': 'id'})
    .merge(iddic, how='left')
)


##
(
    ret[['id', 'name', 'source', 'status', 'cnt']]
    .to_csv('doc/ivst_rank.csv', encoding='utf8',
            sep='\t', index=False)
)


##
# Get title list
titles = coll.distinct('職稱')
df = getdf(
    coll.aggregate([
        {'$group': {'_id': {'id': '$id', 'title': '$職稱'},
                    'cnt': {'$sum': 1}}}
    ], allowDiskUse=True)
).pipe(unwind)

print(df.head())


##
# Count Total Seats
# Consider for empty name but unempty inst. Ex: 86380710.
# Group by id, 姓名, instid
# Remove empty 姓名,職稱,所代表法人. Ex: 28679184.
# Keep duplicate 姓名 by id. Ex: 02562104.
# Remove invalid titles(法人代表、監察人、清理人). Ex: 03705903.

# 重複姓名席次計算：
# 有 instid: 代表多個席次, 應保留
# 無 instid: 可能只是單純重複, 也可能是多個席位，先保留計算

invalid_title = ['接管小組召集人', '法人代表', '監察人', '臨時管理人',
                 '重整人', '重整監督人']

ret = coll.aggregate([
    {'$match': {
        '$or': [
            {'姓名': {'$ne': ''}},
            {'職稱': {'$ne': ''}},
            {'所代表法人': {'$ne': ''}}],
        '職稱': {'$nin': invalid_title}
    }},
    # {'$group': {'_id': {'id': '$id', 'name': '$姓名', 'instid': '$instid'}}},
    {'$group': {'_id': {'id': '$id'},
                'total': {'$sum': 1}}},
])
total_seats = unwind(getdf(ret))


##
# Create Company link and count investment seats
# Keep duplicate 姓名 by id. Ex: 02562104.
# Remove invalid titles(法人代表、監察人、清理人). Ex: 03705903.
coll = db.boards1
ret = coll.aggregate([
    {'$match': {
        'instid': {'$ne': np.nan},
        '職稱': {'$nin': invalid_title},
    }},
    {'$group': {'_id': {'id': '$id', 'name': '$姓名', 'instid': '$instid'}}},
    {'$group': {'_id': {'src': '$_id.instid', 'dst': '$_id.id'},
                'cnt': {'$sum': 1}}},
    # {'$sort': {'cnt': -1}},
])
links = unwind(getdf(ret))

##
links = links.merge(total_seats.rename(columns={'id': 'dst'}), how='left')
if links.total.min() == 0:
    raise Exception('Some company has no seats')

links['seat_ratio'] = links.cnt.astype('float') / links.total
links = (
    links
    .merge(iddic[['id', 'name']]
           .rename(columns={'id': 'src', 'name': 'src_name'}))
    .merge(iddic[['id', 'name']]
           .rename(columns={'id': 'dst', 'name': 'dst_name'}))
)


##
coll = db.comLink1
coll.drop()
coll.insert_many(links.to_dict('record'))
print('%s inserted %s items' % (coll.name, coll.count()))


##
