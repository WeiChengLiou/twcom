#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
from random import choice
from traceback import print_exc

site = u'http://localhost:4000/'
# site = u'http://dataing.pw/'

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
    u'boss?bossid={bossid}',
    ]


for rankby in ('ivst', 'sons', 'inst', 'bosscoms'):
    url = u'rank?data=twcom&rankby={rankby}&n=10'.format(
        rankby=rankby)
    urls.append(url)


def test(k, url0):
    url = site+url0
    req = requests.get(url)
    try:
        print req.text[:1000]
        return json.loads(req.json())
    except:
        print_exc()
        print req.text
        raise Exception(k, url0)


dic = {k: test(k, v) for k, v in urls0.iteritems()}
urls[0] = urls[0].format(name=choice(dic['board']['boards'])['name'])
urls[1] = urls[1].format(target=choice(dic['board']['boards'])['target'])
for i in xrange(2, 5):
    urls[i] = urls[i].format(id=choice(dic['com'].keys()))
urls[5] = urls[5].format(bossid=choice(dic['boss'])['_id'])
[test(i, v) for i, v in enumerate(urls)]
