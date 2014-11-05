#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pdb


class groups(object):
    """Clustering different coms with com connections"""
    def __init__(self):
        self.groups = []

    def find_group(self, id):
        for i, grp in enumerate(self.groups):
            if id in grp:
                return i, grp
        return -1, None

    def add(self, id, id1=None):
        id = unicode(id)

        i, grp = self.find_group(id)
        if grp is None:
            self.groups.append(set([id]))
            i, grp = self.find_group(id)

        if id1 is not None:
            id1 = unicode(id1)
            i1, grp1 = self.find_group(id1)
            if grp1 and i != i1:
                grp.update(grp1)
                del self.groups[i1]
            else:
                grp.add(id1)

    def __iter__(self):
        for r in self.groups:
            yield r

    def __str__(self):
        try:
            li = [u'Group count: {0}'.format(len(self.groups))]
            for grp in self.groups:
                li.append(u'[{0}]'.format(u'\t'.join(grp)))
            return u'\n'.join(li).encode('utf8')
        except:
            pdb.set_trace()


if __name__ == '__main__':
    """"""

