#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
from random import choice
from traceback import print_exc
from pdb import set_trace

site = u'http://localhost:4000/'
#site = u'http://dataing.pw/'

id = '03064421'
name = u'王雪紅'
comname = u'中央投資'

urls0 = {
    'board': u'query?board={id}'.format(id=id),
    'boss': u'query?boss={name}'.format(name=name),
    'com': u'query?com={comname}'.format(comname=comname),
    }

urls = [
    u'com?boss={name}',
    u'com?target={target}',
    u'com?id={id}',
    u'com?comboss={id}',
    u'com?comaddr={id}',
    u'boss?id={id}']


def test(url0):
    url = site+url0
    print url.encode('utf8')
    req = requests.get(url)
    try:
        return json.loads(req.json())
    except:
        print_exc()
        print req.text
        raise Exception()


dic = {k: test(v) for k, v in urls0.iteritems()}
urls[0] = urls[0].format(name=choice(dic['board'])['name'])
urls[1] = urls[1].format(target=choice(dic['board'])['target'])
for i in xrange(2, 6):
    urls[i] = urls[i].format(id=choice(dic['com'].keys()))
map(test, urls)

