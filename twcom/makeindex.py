#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import sys
import gzip
import os
import json
import itertools as it
from glob import glob
import yaml
from twcom.utils import db
CONFIG = yaml.load(open('config.yaml'))
path = 'files'
path = 'TW Company Download/files'
path = '{0}'.format(CONFIG['src'])
print(path)
errcol = {}
errid = {}


fis = glob('{}/*.json.gz'.format(path))
fis = sorted(filter(lambda x: re.search('^\d+', os.path.basename(x)), fis))


##
def getfile(dst):
    print("Please goto 'http://gcis.nat.g0v.tw/'")


def readg(fi):
    # Read gzip file
    def dicfun(li):
        # decode line into key-value pair
        return li[:8].decode('utf8'), json.loads(li[9:])

    try:
        g = gzip.open(os.path.join(path, fi))
        for li in g:
            yield dicfun(li)
    except:
        print(sys.exc_info())
    finally:
        print('close %s' % fi)
        g.close()


def insraw():
    # Insert raw data
    tbl = 'raw'
    db[tbl].drop()
    coll = db[tbl]

    def fun(kv):
        kv[1]['id'] = kv[0]
        return (kv[1])
    kvs = it.chain.from_iterable(map(readg, fis))

    li = []
    for x in map(fun, kvs):
        li.append(x)
        if len(li) == 50000:
            coll.insert_many(li)
            li = []
    if li:
        coll.insert_many(li)


if __name__ == '__main__':
    """"""
    ids = ['75370905', '16095002', '73251209', '75370601']
    # refresh()
