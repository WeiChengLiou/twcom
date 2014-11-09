#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twcom.query import *
import test.test_query as tq
#import test.test_nbutils as tnb
import matplotlib as mpl

font = 'AR PL KaitiM Big5'
mpl.rcParams['font.sans-serif'] = font

name = [u'王雪紅']
ids = ['04278323', '28428379']

# G = get_bossnet_boss(name)
# nx.draw_graphviz(G)
# plt.show()

# tq.test_comboss(ids[0])
tq.test_com_network()
tq.test_boss_network()
