#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json

site = u'http://localhost:4000/'
#site = u'http://dataing.pw/'

urls0 = {
    'board': u'query?board=03064421',
    }

urls = [
    u'com?boss=王雪紅',
    u'com?target={target}',
    u'com?id=03064421',
    u'com?comboss=03064421',
    u'com?comaddr=03064421',
    u'boss?id=03064421']


def test(url0):
    url = site+url0
    print url.encode('utf8')
    req = requests.get(url)
    assert(len(req.text) > 1000)
    return json.loads(req.json())


dic = test(urls0['board'])
urls[1] = urls[1].format(target=dic[0]['target'])
map(test, urls)

