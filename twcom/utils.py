#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pdb import set_trace
from traceback import print_exc
import logging
import yaml
from os.path import exists
from twcom.work import yload
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


def init(db):
    "init mongodb db class"
    pwdfi = '../pwd.yaml'
    if not exists(pwdfi):
        pwdfi = 'pwd.example.yaml'
    dic = yaml.load(open(pwdfi))
    uri = 'mongodb://{user}:{pwd}@{ip}:{port}/{db}'.format(**dic)
    print uri
    return MongoClient(uri)[db]


CONFIG = yaml.load(open('config.yaml'))
db = init(CONFIG['db'])


def bad_board(db):
    # get bad board name from badmark()
    badmark = yload('doc/badmark.yaml')
    condic = {'$or': [
        {'name': {'$regex': key}} for key in badmark]}
    ret = list(db.boards.find(condic, ['name']).distinct('name'))
    ret.extend([u'', u'缺'])
    return ret


def getname(id):
    # get company name by id
    # if not found, return id
    ret = db.cominfo.find_one({'id': id}, ['name'])
    if ret:
        return ret['name']
    else:
        return id


def getnamedic(ids):
    # get company name by multiple ids
    # if not found, return id
    # return dictionary(id, name)
    if not hasattr(ids, '__iter__'):
        ids = (ids,)
    ret = db.cominfo.find({'id': {'$in': ids}}, ['id', 'name'])
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
            print u'Warnning: duplicte id - {0}'.format(name)
            return ret['id'][0]
    else:
        return name


def getbadcoms():
    bads = yload('doc/badstatus.yaml')
    cond = {'status': {'$in': list(bads)}}
    return set([r['id'] for r in db.cominfo.find(cond)])


def insitem(db, coll, item):
    try:
        db[coll].insert(item)
    except DuplicateKeyError:
        """"""
    except:
        print_exc()
        set_trace()
