#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from markdown import markdown
from IPython.display import HTML
output_path = 'output'

import pandas as pd
from matplotlib import pylab as plt
import networkx as nx
import itertools as it
#from twcom.utils import *
from twcom import utils
from twcom import work


def rendermd(markdown_str):
    print 'Deprecated rendermd'
    if hasattr(markdown_str, '__iter__'):
        markdown_str = u''.join(markdown_str)
    return HTML(u"<p>{}</p>".format(markdown(markdown_str)))


class ListTable(list):
    """
    Overridden list class which takes a 2-dimensional list of
    the form [[1,2,3],[4,5,6]], and renders an HTML Table in
    IPython Notebook.
    """

    def _repr_html_(self):
        html = [u"<table>"]
        for row in self:
            html.append(u"<tr>")

            for col in row:
                html.append(u"<td>{0}</td>".format(unicode(col)))

            html.append(u"</tr>")
        html.append(u"</table>")
        return u''.join(html)


def table_attr(s, border=1):
    print 'Deprecated table_attr'
    return s.replace(u'<table>', u'<table border="{0}">'.format(border))


def html_template(htmlstr, **kwargs):
    # template to render string to html
    return u"""<!DOCTYPE: html>
        <meta charset="utf-8"><style>
        table {{
        border-collapse: collapse;
        }}
        td {{
        padding: 5px;
        }}
        table,th,td
        {{
        border:1px solid black;
        }}
        </style>
        <html>
        <body>
        {0}
        </body>
        </html>""".format(u''.join(map(unicode, htmlstr)))


def writehtml(li, fi):
    # Export html
    with open(fi, 'wb') as f:
        f.write(html_template(ListTable(li)._repr_html_()).encode('utf8'))


def writecsv(li, fi):
    # Export csv
    with open(fi, 'wb') as csvfile:
        csvwriter = csv.writer(
            csvfile, delimiter=',',
            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for r in li:
            csvwriter.writerow(map(lambda x: unicode(x).encode('utf8'), r))


def htmlcsv(msg, df, fi):
    # Export DataFrame as html and csv
    # add msg as intro to hyperlink
    html_url = u"{0}/html/{1}.html".format(output_path, fi)
    csv_url = u"{0}/csv/{1}.csv".format(output_path, fi)
    writehtml(df, html_url)
    writecsv(df, csv_url)
    markdown_str = u"{0}：[HTML]({1}), [CSV]({2})".format(
        msg, html_url, csv_url)
    return HTML(u"{}</p>".format(markdown(markdown_str)))


def hideaxis(pos=None):
    # hide x y axis
    if pos:
        df = pd.DataFrame(pos.values(), columns=['x', 'y'])
        plt.xlim([df['x'].min()-5, df['x'].max()+5])
        plt.ylim([df['y'].min()-5, df['y'].max()+5])
    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())


def getcentral(g1):
    # get different centrality
    return pd.DataFrame({
        u'anc': {x: len(nx.ancestors(g1, x)) for x in g1.nodes()},
        u'des': {x: len(nx.descendants(g1, x)) for x in g1.nodes()},
        u'indeg': g1.in_degree(),
        u'outdeg': g1.out_degree()
        })


def printdf(s, coldic=None):
    # print DataFrame with coldic
    x = ListTable()
    cols = []
    for k in s.columns:
        if coldic and k in coldic:
            cols.append(coldic[k])
        else:
            cols.append(k)
    x.append(cols)

    for k, rs in s.iterrows():
        row = []
        for r in rs:
            if hasattr(r, '__iter__'):
                row.append(u','.join(map(unicode, r)))
            else:
                row.append(r)
        x.append(row)
    return x


def drawcom(g, ids=None, **kwargs):
    assert(len(g.node) > 0)
    if ids is None:
        ids = g.node.keys()
    fids = it.ifilter(lambda x: x in ids, g.nodes())
    namedic = {k: work.fixname(v) for k, v in
               utils.getnamedic(list(fids)).iteritems()}
    draw(g, namedic, **kwargs)


def drawboss(g, names=None, **kwargs):
    assert(len(g.node) > 0)
    if not names:
        names = g.node.keys()
    draw(g, names, **kwargs)


def draw(g, lbldic=None, **kwargs):
    # for drawing social network
    node_color = map(lambda k: k[1].get('group', 0), g.node.iteritems())
    pos = nx.graphviz_layout(g)
    vmax = kwargs.get('vmax', max(node_color))
    node_size = kwargs.get('node_size', 40)
    cmap = kwargs.get('cmap', plt.cm.jet)
    alpha = kwargs.get('alpha', 0.5)

    nx.draw_networkx_edges(
        g, pos,
        with_labels=False,
        alpha=alpha,
        edge_width=0.5,
        edge_color='y',
        )
    nx.draw_networkx_nodes(
        g, pos,
        alpha=alpha,
        node_color=node_color,
        vmin=0,
        vmax=vmax,
        cmap=cmap,
        node_size=node_size,
        )
    nx.draw_networkx_labels(
        g, pos, labels=lbldic)
    if kwargs.get('hideaxis', True):
        hideaxis(pos)


def draw_scatter(g):
    fig, ax = plt.subplots(1)
    s = [len(v.get('titles', []))*20 for v in g.node.values()]
    deg = nx.degree(g)
    x = [deg[k] for k in g.node]
    betw = nx.closeness_centrality(g.to_undirected())
    y = [betw[k] for k in g.node]
    c = [v.get('group', 0) for v in g.node.values()]
    ax.scatter(x, y, c=c, cmap=plt.cm.jet, vmin=min(c),
               vmax=max(c), s=s, alpha=0.3)
    dic = defaultdict(list)
    for i, (k, v) in enumerate(g.node.iteritems()):
        pos = (x[i], round(y[i], 2))
        if (pos[0] >= 10 and pos[1] >= 0.45):
            dic[pos].append(v['name'])
    for pos, v in dic.iteritems():
        msg = u'\n'.join(v)
        ax.text(pos[0], pos[1], msg, fontsize=16)

        
def draw_scatter1(g, sizefun=None, lblfun=None):
    fig, ax = plt.subplots(1)
    #s = [len(v.get('titles', []))*20 for v in g.node.values()]
    s = map(sizefun, g.node.values())
    deg = nx.degree(g)
    x = [deg[k] for k in g.node]
    betw = nx.closeness_centrality(g.to_undirected())
    y = [betw[k] for k in g.node]
    c = [v.get('group', 0) for v in g.node.values()]
    ax.scatter(x, y, c=c, cmap=plt.cm.jet,
               vmin=min(c), vmax=max(c), s=s, alpha=0.3)
    if not lblfun:
        return

    dic = defaultdict(list)
    for i, (k, v) in enumerate(g.node.iteritems()):
        pos = (x[i], round(y[i], 2))
        if lblfun(pos, v):
        #if (pos[0]>=5 and pos[1]>=0.35) or len(v.get('titles', 0))>=5:
            dic[pos].append(v['name'])
        
    for pos, v in dic.iteritems():
        ax.text(pos[0], pos[1], u'\n'.join(v), fontsize=16)

