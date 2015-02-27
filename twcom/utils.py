#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId
from pdb import set_trace
from traceback import print_exc
import logging
import yaml
from os.path import exists
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
    pwdfi = '../pwd1.yaml'
    if not exists(pwdfi):
        pwdfi = 'pwd.example.yaml'
    dic = yaml.load(open(pwdfi))
    uri = 'mongodb://{user}:{pwd}@{ip}:{port}/{db}'.format(**dic)
    return MongoClient(uri)[db]


CONFIG = yaml.load(open('config.yaml'))
cn = init(CONFIG['db'])


def badmark():
    return u'暫缺', u'缺額', u'懸缺', u'死亡', u'補選', u'禁止',\
        u'解任', u'請辭', u'註銷', u'停權', u'禁止', u'法拍',\
        u'撤銷', u'當然任', u'不存在', u'臨時', u'辭職'


def bad_board(cn):
    # get bad board name from badmark()
    condic = {'$or': [
        {'name': {'$regex': key}} for key in badmark()]}
    ret = list(cn.boards.find(condic, ['name']).distinct('name'))
    ret.extend([u'', u'缺'])
    return ret


def getname(id):
    # get company name by id
    # if not found, return id
    ret = cn.cominfo.find_one({'id': id}, ['name'])
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
    ret = cn.cominfo.find({'id': {'$in': ids}}, ['id', 'name'])
    dic = {id: id for id in ids}
    for r in ret:
        dic[r['id']] = r['name']
    return dic


def getid(name):
    # get company id by name
    ret = cn.iddic.find_one({'name': name})
    if ret:
        if len(ret['id']) == 1:
            return ret['id'][0]
        else:
            print u'Warnning: duplicte id - {0}'.format(name)
            return ret['id'][0]
    else:
        return name


def badstatus(cn):
    # return bad company status
    status = set()
    status.update(cn.cominfo.find(
        {'status': {'$not': re.compile(u'核准')}}).distinct('status'))
    status.update(cn.cominfo.find(
        {'$or': [{'status': {'$regex': u'停業'}},
         {'status': {'$regex': u'解散'}}]}).distinct('status'))
    return status


def insitem(cn, coll, item):
    try:
        cn[coll].insert(item)
    except DuplicateKeyError:
        """"""
    except:
        print_exc()
        set_trace()
