#!/usr/bin/env python
# -*- coding: utf-8 -*-
import jinja2
import os
import json
import pdb
from traceback import print_exc

template_dir = os.path.join(os.path.dirname(__file__), 'templates')
jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(template_dir),
    autoescape=True)


def render_str(template, **params):
    t = jinja_env.get_template(template)
    return t.render(params)


def render_html(fi, **params):
    try:
        with open(fi, 'wb') as f:
            f.write(render_str('force_layout.html', **params))
    except:
        print_exc()
        pdb.set_trace()


def exp_graph(G, jsonfi):
    for k, v in G.node.iteritems():
        if 'size' not in v:
            v['size'] = 10
        if 'group' not in v:
            v['group'] = 0

    dicBig = {}
    dic0 = [dic for k, dic in G.node.iteritems()]
    dicBig['nodes'] = dic0
    idx = list(G.node.keys())
    dic1 = []
    for x in G.edge:
        for y in G.edge[x]:
            dic = {'source': idx.index(x), 'target': idx.index(y)}
            for k, v in G.edge[x][y].iteritems():
                dic[k] = v
            dic1.append(dic)
    if len(dic1) > 0:
        dic1.append(dic1[0])
    dicBig['links'] = dic1
    json.dump(dicBig, open(jsonfi, 'wb'))


def test():
    dic = {'nodes': [
        {'size': 1, 'group': 1, 'name': 'a'},
        {'size': 1, 'group': 1, 'name': 'b'}],
        'links': [{'source': 1, 'target': 0, 'weight': 1},
                  {'source': 1, 'target': 0, 'weight': 1}]}

    jsonfi = 'test.json'
    fi = 'test.html'
    json.dump(dic, open(jsonfi, 'wb'))
    color = 'category20b'

    render_html(jsonfi, fi, color=color)


if __name__ == '__main__':
    """"""
    #test()
