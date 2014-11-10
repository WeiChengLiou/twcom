#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twcom.utils import *
from twcom.query import *
import test.test_query as tq
#import test.test_nbutils as tnb
import matplotlib as mpl
from matplotlib import pylab as plt

#font = 'AR PL KaitiM Big5'
#mpl.rcParams['font.sans-serif'] = font

name = [u'王雪紅']
ids = ['04278323', '28428379', u'XX基金會']

#G = get_bossnet_boss(name)
#nx.draw_graphviz(G)
#plt.show()

#tq.test_comboss()

# 產生公司關係圖
tq.test_com_network()

# 產生董監事關係圖
tq.test_boss_network()
