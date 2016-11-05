#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twcom import makeindex as mi
from twcom import run_board_target as runbd
from twcom import ranks


mi.insraw()
mi.refresh()
mi.genbadstatus()
mi.fixing1()
mi.fixing()

runbd.resetComnetBoss()
ranks.inscomrank()

# runbd.test()
