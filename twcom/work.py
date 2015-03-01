#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import inspect
from collections import defaultdict
import itertools as it


def groupdic(vals, key):
    dic = defaultdict(list)
    [dic[key(r)].append(r) for r in vals]
    return dic


def get_funname():
    return inspect.stack()[1][3]


def chunk(x, n=1):
    """ Split list into every size-n chunk """
    for i in xrange(0, len(x), n):
        yield x[i:(i+n)]


def take(li, n):
    fun = lambda y: y[0] < n
    for i, x in it.ifilter(fun, enumerate(li)):
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


def sprint(args, **kwargs):
    if 'sep' in kwargs:
        sep = kwargs['sep']
    else:
        sep = u'\t'
    if 'lvl' in kwargs:
        lvl = kwargs['lvl']
    else:
        lvl = 0

    if not hasattr(args, '__iter__'):
        print u' '*lvl, unicode(args)
        return
    elif isinstance(args, dict):
        for k, v in args.iteritems():
            sprint([k, ':', v], lvl=lvl+1)
        return

    li = []
    justPrint = True
    for i, x in enumerate(args):
        if i == 0:
            if hasattr(x, '__iter__'):
                justPrint = False

        if not justPrint:
            sprint(x, lvl=lvl+1)
        else:
            li.append(unicode(x))
    if li:
        print u' '*lvl, sep.join(li)


def lprint(*args, **kwargs):
    sprint(list(flatten(args)), **kwargs)


def dicprint(args, **kwargs):
    if 'sep' in kwargs:
        sep = kwargs['sep']
    else:
        sep = u'\t'
    for k, v in args.iteritems():
        lprint(k, v, sep=sep)


def pdic(dic, space=0):
    blank = u' ' * (1+space)
    if isinstance(dic, dict):
        li = [u'{\n']
        for k, v in dic.iteritems():
            li.append(u'{0}{1}:{2}'.format(blank, k, pdic(v, space+1)))
        li.append(blank + u'}\n')
        return u''.join(li)
    elif hasattr(dic, '__iter__') and (not isinstance(dic, basestring)):
        li = [u'[']
        li.append(u','.join(map(pdic, dic)))
        li.append(u']\n')
        return u''.join(li)
    else:
        return u'{0}{1}\n'.format(u' '*space, dic)


def replaces(str0, words):
    return reduce(lambda x, y: x.replace(y, u''), words, str0)


def fixname(name):
    # return reduced company name
    return replaces(name, (u'股份', u'有限', u'公司'))


def lr_intr(l, r):
    return set(l).intersection(r)


if __name__ == '__main__':
    """"""
    # test()
    # if len(l)==0 and len(r)==0:return None
