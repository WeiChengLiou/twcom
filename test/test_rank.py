#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from twcom import ranks


class rankTestCase(unittest.TestCase):
    def test_rankivst(self):
        res = ranks.rankivst()
        print('begin')
        print(res)
        self.assertEquals(len(res), 10)
        self.assertEquals(len(res.columns), 4)
