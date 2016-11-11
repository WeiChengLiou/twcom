#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import inspect
from collections import defaultdict
import itertools as it
import time
import cPickle
import gzip
from traceback import print_exc
from pdb import set_trace
import yaml
import json


def timeit(func):
    def run(*args, **kwargs):
        t0 = time.time()
        ret = func(*args, **kwargs)
        t1 = time.time()
        print 'Running {0:1.4f} seconds'.format(t1 - t0)
        return ret
    return run


def groupdic(iters, key):
    dic = defaultdict(list)
    [dic[key(r)].append(r) for r in iters]
    return dic


def get_funname():
    return inspect.stack()[1][3]


def getitem(dicli, col):
    return [r[col] for r in dicli]


def chunk(x, n=1):
    """ Split list into every size-n chunk """
    for i in xrange(0, len(x), n):
        yield x[i:(i+n)]


def take(li, n):
    for i, x in it.ifilter(lambda y: y[0] < n, enumerate(li)):
        yield x


def liget(li, key, default):
    return [r.get(key, default) for r in li]


def getdf(ret):
    return pd.DataFrame(list(ret))


def flatten(vals):
    for x in vals:
        if hasattr(x, '__iter__'):
            for y in flatten(x):
                yield y
        else:
            yield x


def strin(s1, strs):
    for x in strs:
        if x in s1:
            return True
    return False


def show(dic):
    print json.dumps(
        dic,
        sort_keys=True,
        ensure_ascii=False,
        indent=2
    )


def replaces(str0, words):
    return reduce(lambda x, y: x.replace(y, u''), words, str0)


def fixname(name):
    # return reduced company name
    return replaces(name, (u'股份', u'有限', u'公司'))


def lr_intr(l, r):
    return set(l).intersection(r)


def trytest(func):
    def run(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            print_exc()
            set_trace()
    return run


def deprecated(func):
    def run(*args, **kwargs):
        print func.__name__, 'is going to be deprecated.'
        return func(*args, **kwargs)
    return run


def save(obj, fi):
    cPickle.dump(obj, gzip.open(fi, 'wb'))
    print '%s saved' % fi


def load(fi):
    return cPickle.load(gzip.open(fi, 'rb'))


def yload(fi):
    return yaml.load(open(fi))


def ysave(obj, fi, debug=0):
    def toyaml(obj, lvl=0):
        if isinstance(obj, list) or isinstance(obj, set):
            for x in obj:
                if x == u'':
                    yield u'%s- ""' % (u' ' * lvl)
                else:
                    yield u'%s- %s' % (u' ' * lvl, x)
        elif isinstance(obj, dict):
            for x in obj:
                v = obj[x]
                if hasattr(v, '__iter__'):
                    yield u'%s%s:' % (u' ' * lvl, x)
                    for v1 in toyaml(v, lvl+1):
                        yield v1
                elif v == u'':
                    yield u'%s%s: ""' % (u' ' * lvl, x)
                else:
                    yield u'%s%s: %s' % (u' ' * lvl, x, v)

    with open(fi, 'wb') as f:
        s = u'\n'.join(list(toyaml(obj)))
        if debug:
            print s
        else:
            f.write(s.encode('utf8'))
    print '%s saved' % fi


def yread(fi):
    with open(fi) as f:
        for li in f:
            if '#' in li[0]:
                continue
            yield li.replace('\n', '').decode('utf8')


if __name__ == '__main__':
    """"""
    # test()
    # if len(l)==0 and len(r)==0:return None
