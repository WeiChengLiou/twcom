#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pdb import set_trace
from traceback import print_exc
import logging
import yaml
from os.path import exists
from twcom.work import yload, yread
logger = logging.getLogger('twcom')

# create console handler and set level to info
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# create error file handler and set level to error
handler = logging.FileHandler("twcom.log", "w", encoding=None, delay="true")
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


bad_boards = yread('doc/bad_boards.yaml')
rx_bad_boards = re.compile(u'|'.join(bad_boards), re.UNICODE)


def chk_board(name):
    # Check board name are right
    if name:
        return not rx_bad_boards.search(name)
    else:
        return False


def init(db):
    "init mongodb db class"
    pwdfi = 'pwd.yaml'
    if not exists(pwdfi):
        raise Exception('Missed password file. Make one from pwd.yaml.example')
    dic = yaml.load(open(pwdfi))
    uri = 'mongodb://{user}:{pwd}@{ip}:{port}/{db}'.format(**dic)
    return MongoClient(uri)[db]


CONFIG = yaml.load(open('config.yaml'))
db = init(CONFIG['db'])


def getname(id, cn=None):
    # get company name by id
    # if not found, return id
    if not cn:
        cn = db.cominfo
    ret = cn.find_one({'id': id}, ['name'])
    if ret:
        return ret['name']
    else:
        return id


def getnamedic(ids, cn=None):
    # get company name by multiple ids
    # if not found, return id
    # return dictionary(id, name)
    if not cn:
        cn = db.cominfo
    if not hasattr(ids, '__iter__'):
        ids = (ids,)
    ret = cn.find({'id': {'$in': ids}}, ['id', 'name'])
    dic = {id: id for id in ids}
    for r in ret:
        dic[r['id']] = r['name']
    return dic


def getid(name):
    # get company id by name
    ret = db.iddic.find_one({'name': name})
    if ret:
        if len(ret['id']) == 1:
            return ret['id'][0]
        else:
            print(u'Warnning: duplicte id - {0}'.format(name))
            return ret['id'][0]
    else:
        return name


def getbadcoms(cn=None):
    if not cn:
        cn = db.cominfo
    bads = yload('doc/badstatus.yaml')
    cond = {'status': {'$in': list(bads)}}
    return set([r['id'] for r in cn.find(cond)])


def insitem(db, coll, item):
    try:
        db[coll].insert(item)
    except DuplicateKeyError:
        """"""
    except:
        print_exc()
        set_trace()
