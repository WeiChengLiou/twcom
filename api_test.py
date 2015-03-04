#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

site = u'http://localhost:4000/'
urls = [
    u'com?id=03064421',
    u'com?boss=王雪紅',
    u'com?target=547995c687f44634c15ecead',
    u'com?comboss=03064421',
    u'com?comaddr=03064421',
    u'boss?id=03064421']

for url0 in urls:
    url = site+url0
    print url
    req = requests.get(url)
    assert(len(req.text) > 1000)
