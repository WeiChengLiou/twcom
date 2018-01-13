#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pdb import set_trace
from collections import defaultdict


class sparse_mat(object):
    @classmethod
    def fromdict(cls, dic):
        obj = sparse_mat()
        for k, v in dic.items():
            obj.add(k, v)
        return obj

    @classmethod
    def fromlist(cls, lis):
        obj = sparse_mat()

        def add(xy):
            x, y = xy
            obj.xdic[x].add(y)
            obj.ydic[y].add(x)
        map(add, lis)
        return obj

    def __init__(self):
        self.xdic = defaultdict(set)
        self.ydic = defaultdict(set)

    def add(self, k, v):
        self.xdic[k].update(v)
        [self.ydic[y].add(k) for y in v]

    def adddic(self, k, dic, keycol):
        self.xdic[k] = dic
        [self.ydic[y].add(k) for y in dic[keycol]]

    def get_xlike(self, x):
        xs = set()
        for y in self.xdic[x]:
            for xi in filter(lambda z: z != x, self.ydic[y]):
                xs.add(xi)
        return xs

    def get_ylike(self, y):
        ys = set()
        for x in self.ydic[y]:
            for yi in filter(lambda z: z != y, self.xdic[x]):
                ys.add(yi)
        return ys

    def jaccard(self, x1, x2):
        try:
            return float(len(self.intersec(x1, x2))) /\
                len(self.xdic[x1].union(self.xdic[x2]))
        except:
            print(x1, self.xdic[x1])
            print(x2, self.xdic[x2])
            set_trace()

    def intersec(self, x1, x2):
        return self.xdic[x1].intersection(self.xdic[x2])

    @property
    def nodes(self):
        return self.xdic.keys()

    @property
    def links(self):
        keys = sorted(self.nodes)
        for k in keys:
            ks = set()
            for y in self.xdic[k]:
                for ki in filter(lambda z: z > k, self.ydic[y]):
                    ks.add(ki)
            for k2 in ks:
                yield (k, k2)


def test():
    def sethash(x):
        return hash('/'.join(sorted(map(str, x))))

    dic = {'a': [1, 2, 3],
           'b': {2, 3, 5},
           'c': {5, 6, 7},
           'd': {4, 7}
           }
    obj = sparse_mat.fromdict(dic)
    assert sethash(obj.get_xlike('a')) == sethash({'b'})
    assert sethash(obj.get_xlike('c')) == sethash({'b', 'd'})
    assert sethash(obj.get_ylike(2)) == sethash({1, 3, 5})
    print(obj.xdic)
    print(obj.ydic)

    print(sorted(obj.nodes))
    for l in obj.links:
        print(l, obj.xdic[l[0]], obj.xdic[l[1]], obj.jaccard(*l),
              len(obj.intersec(*l)))


if __name__ == '__main__':
    test()
