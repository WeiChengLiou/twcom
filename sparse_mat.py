#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict
import itertools as it


class sparse_mat(object):
    @classmethod
    def fromdict(cls, dic):
        obj = sparse_mat()
        for k, v in dic.iteritems():
            obj.xdic[k] = set(v)
            [obj.ydic[y].add(k) for y in v]
        return obj

    @classmethod
    def fromlist(cls, lis):
        obj = sparse_mat()
        for x, y in lis:
            obj.xdic[x].add(y)
            obj.ydic[y].add(x)
        return obj

    def __init__(self):
        self.xdic = defaultdict(set)
        self.ydic = defaultdict(set)

    def get_xlike(self, x):
        xs = set()
        for y in self.xdic[x]:
            for xi in it.ifilter(lambda z: z != x, self.ydic[y]):
                xs.add(xi)
        return xs

    def get_ylike(self, y):
        ys = set()
        for x in self.ydic[y]:
            for yi in it.ifilter(lambda z: z != y, self.xdic[x]):
                ys.add(yi)
        return ys

    def jaccard(self, x1, x2):
        return len(self.xdic[x1] & self.xdic[x2]) /\
            len(self.xdic[x1] | self.xdic[x2])

    def intercnt(self, x1, x2):
        return len(self.xdic[x1] & self.xdic[x2])


def test():
    dic = {'a': [1, 2, 3],
           'b': {2, 3, 4},
           'c': {5, 6, 7}}
    obj = sparse_mat.fromdict(dic)
    print obj.get_xlike('a')
    print obj.get_xlike('c')
    print obj.xdic
    print obj.ydic

if __name__ == '__main__':
    test()
