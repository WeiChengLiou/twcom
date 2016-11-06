#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
from twcom.query import getComNet
from matplotlib import pylab as plt
import matplotlib as mpl
# import seaborn as sb
from twcom.work import replaces
from twcom.utils import db
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from os.path import join
font = 'AR PL KaitiM Big5'
mpl.rcParams['font.sans-serif'] = font
srcpath = '/home/gilbert/meeting/ZongRong/'
# sb.set(font=font)


def getComNet2(ids):
    frep = lambda name: replaces(name, [u'股份', u'有限', u'公司'])
    g = getComNet(ids)
    namedic = {}
    ret = db.cominfo.find({'id': {'$in': ids}})
    namedic = {r['id']: frep(r['name']) for r in ret}
    for r in ids:
        if r not in namedic:
            namedic[r] = r
    nx.set_node_attributes(g, 'name', namedic)
    return g


def figsave(show, fi):
    if show == 1:
        plt.show()
    else:
        plt.savefig(fi)
    plt.close()


def draw_com(G, labels, show, fi, nolabel=True):
    pos = graphviz_layout(G)
    nodes = labels.keys()
    nodes1 = [k for k in G.node if k not in labels]
    print G.number_of_nodes(), map(len, (nodes, nodes1))
    nx.draw_networkx_nodes(
        G, pos,
        with_labels=False,
        node_size=36,
        nodelist=nodes,
        node_color='b',
        )
    nx.draw_networkx_nodes(
        G, pos,
        with_labels=False,
        node_size=16,
        alpha=0.5,
        nodelist=nodes1,
        node_color='r',
        )
    nx.draw_networkx_edges(
        G, pos,
        alpha=0.3,
        width=1,
        )
    if not nolabel:
        nx.draw_networkx_labels(
            G, pos, labels,
            font_size=10,)
    frame = plt.gca()
    frame.axes.get_xaxis().set_visible(False)
    frame.axes.get_yaxis().set_visible(False)
    figsave(show, fi)


def txtload(fi):
    with open(fi, 'rb') as f:
        li = [r.replace('\n', '').decode('utf8') for r in f]
    return li


def gen_com(fi, **kwargs):
    fi = join(srcpath, fi)
    fi1 = fi.replace('txt', 'png')
    ids = txtload(fi)
    g = getComNet2(ids)
    labels = {id: g.node[id]['name'] for id in ids}
    draw_com(g, labels, 0, fi1,
             nolabel=bool(kwargs.get('nolabel', True)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('fun', type=str)
    parser.add_argument('-fi', type=str)
    parser.add_argument('-nolabel', type=int, default=1)
    args = parser.parse_args()
    fun = eval(args.fun)
    fun(**vars(args))
