#!/usr/bin/env python
# -*- coding: utf-8 -*-
import jinja2
from os.path import join, dirname
import json
import pdb
from traceback import print_exc

template_dir = join(dirname(__file__), 'templates')
jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(template_dir),
    autoescape=True)


def render_str(template, **params):
    t = jinja_env.get_template(template)
    return t.render(params)


def render_html(fi, **params):
    template = 'force_layout.html'
    try:
        with open(fi, 'wb') as f:
            f.write(render_str(template, **params))
    except:
        print_exc()
        pdb.set_trace()


def write_d3(fi, **kwargs):
    # Export d3 file, return htmrul
    path = kwargs.get('path', '')
    jsonfi = fi + '.json'
    htmlfi = join(path, fi + '.html')

    render_html(htmlfi, FileName=jsonfi, **kwargs)
    return unicode(htmlfi)


def exp_graph(G, **kwargs):
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

    if 'fi' in kwargs:
        jsonfi = join(kwargs.get('path', ''), kwargs.get('fi') + '.json')
        json.dump(dicBig, open(jsonfi, 'wb'))
    else:
        return json.dumps(dicBig)


if __name__ == '__main__':
    """"""
    #test()
