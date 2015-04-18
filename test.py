#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twcom.utils import *
from twcom.query import *
import test.test_query as tq
import test.test_rank as trank
import unittest
import inspect


name = [u'王雪紅']
ids = ['04278323', '28428379', u'XX基金會']
ids = ['03538906', '財團法人生物技術開發中心']


#tq.test_comboss()

# 產生公司關係圖
#tq.test_com_network()

# 產生董監事關係圖
#tq.test_boss_network()


# 排行榜
# suite = (unittest.TestLoader()
#          .loadTestsFromTestCase(trank.rankTestCase))
# unittest.TextTestRunner(verbosity=2).run(suite)


def trytest(func):
    def run(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            print_exc()
            set_trace()
    return run


def test():
    print 'test'

