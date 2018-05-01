# from twcom.query import getcomboss
# from twcom.work import show
import pandas as pd
from collections import defaultdict
import unittest
from unittest import mock
from twcom.query import get_boss_node, get_boss_network, db
from twcom.query import get_board_list, get_board_ref, get_com_name
from bson.objectid import ObjectId
db, get_board_list, get_board_ref, get_com_name


def getObjectId(x):
    if isinstance(x, ObjectId):
        return x
    else:
        return ObjectId(('0' * 24 + x)[-24:])


sample_coms = []
for com in ('0', '1', '2', '3'):
    sample_coms.append({
        'id': com,
        'name': 'com' + com,
    })
test_boards = pd.DataFrame([
    ('a', '0', 'boss1'),
    ('b', '0', 'boss2'),
    ('a', '1', 'boss1'),
    ('c', '1', 'boss2'),
    ('b', '2', 'boss1'),
    ('c', '2', 'boss2'),
    ('d', '2', 'boss2'),
    ('e', '3', 'boss1'),
    ('f', '3', 'boss2'),
], columns=['name', 'id', 'title'])
test_boards['ref'] = test_boards['name'].apply(getObjectId)
test_boards = test_boards.to_dict('record')


def board_list(coms):
    dic = defaultdict(list)
    for x in filter(lambda x: x['id'] in coms, test_boards):
        dic[x['id']].append(getObjectId(x['ref']))
    for k, v in dic.items():
        yield {'_id': k, 'refs': v}


def board_ref(refs):
    dic = defaultdict(list)
    for x in filter(lambda x: x['ref'] in refs, test_boards):
        dic[x['ref']].append(x)
    for ref, li in dic.items():
        yield {
            '_id': getObjectId(ref),
            'coms': [x['id'] for x in li],
            'title': [x['title'] for x in li],
        }


def com_name(coms):
    for x in filter(lambda x: x['id'] in coms, sample_coms):
        yield x


class testQuery(unittest.TestCase):
    @mock.patch('twcom.query.get_com_name')
    @mock.patch('twcom.query.get_board_ref')
    @mock.patch('twcom.query.get_board_list')
    @mock.patch('twcom.query.get_boss_node')
    def test_get_boss_network(self, mock_func1, mock_func2,
                              mock_func3, mock_func4):
        name = '王雪紅'
        mock_func1.return_value = map(getObjectId, ['a', 'e'])
        mock_func2.side_effect = board_list
        mock_func3.side_effect = board_ref
        mock_func4.side_effect = com_name

        G = get_boss_network(names=[name], maxlvl=2)
        self.assertEqual(G.number_of_nodes(), 6)

    @mock.patch('twcom.query.db')
    def test_get_bosslike(self, mock_db):
        name = '王雪紅'
        func1 = mock_db.boards1.find
        func2 = func1({'姓名': name}).distinct
        func2.return_value = ['a', 'b']

        cnt = len(list(get_boss_node(name)))
        self.assertEqual(cnt, 2)
        func1.assert_called_with({'姓名': name})


if __name__ == '__main__':
    unittest.main()
