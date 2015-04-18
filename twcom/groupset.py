#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pdb import set_trace
import abc


class cusset(set):
    pass


class groupset(abc.types.ListType):
    """Clustering different coms with com connections"""
    @classmethod
    def fromdict(cls, dic):
        obj = groupset()
        obj.update(dic)

    def getgrp(self, id):
        for grp in self:
            if id in grp:
                return grp
        return None

    def update(self, dic):
        for k, v in dic.iteritems():
            if v:
                self.add(k, v)

    def add(self, id, *args, **kwargs):
        id = unicode(id)
        grp = self.getgrp(id)
        if grp is None:
            self.append(cusset([id]))
            grp = self.getgrp(id)

        for id1 in args:
            id1 = unicode(id1)
            grp1 = self.getgrp(id1)
            if grp1 and (grp != grp1):
                grp.update(grp1)
                self.remove(grp1)
            else:
                grp.add(id1)
        for k, v in kwargs:
            setattr(grp, k, v)

    def __str__(self):
        try:
            li = [u'Group count: {0}'.format(len(self))]
            for grp in self:
                li.append(u'[{0}]'.format(u'\t'.join(grp)))
            return u'\n'.join(li).encode('utf8')
        except:
            set_trace()

    def export(self, fi):
        with open(fi, 'wb') as f:
            for ks in self:
                ks1 = sorted(ks)
                k0 = ks1.pop()
                for k in ks1:
                    f.write(u'{0}:{1}\n'.format(k0, k).encode('utf8'))


def test():
    grps = groupset()
    grps.add('a', 'b')
    grps.add('c')
    grps.add('d','e')

    for k in grps:
        setattr(k, '_id', ''.join(k))
        print k, k._id

if __name__ == '__main__':
    """"""
    test()

