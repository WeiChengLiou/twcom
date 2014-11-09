#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from markdown import markdown
from IPython.display import HTML
import os
from visualize.output import render_html
output_path = 'output'
urlpath = output_path

import pandas as pd
import matplotlib as mpl
from matplotlib import pylab as plt
import networkx as nx
import itertools as it
from twcom.utils import *


def rendermd(markdown_str):
    print 'Deprecated'
    if hasattr(markdown_str, '__iter__'):
        markdown_str = u''.join(markdown_str)
    return HTML(u"<p>{}</p>".format(markdown(markdown_str)))


class ListTable(list):
    """ Overridden list class which takes a 2-dimensional list of 
    the form [[1,2,3],[4,5,6]], and renders an HTML Table in 
    IPython Notebook. """

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
    print 'Deprecated'
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


def write_d3(fi, dir, **kwargs):
    # Export d3 file, return htmrul
    fi1 = fi.replace('json', 'html')
    htmlfi = os.path.join(output_path, dir, fi1)
    htmlurl = os.path.join(urlpath, dir, fi1)

    render_html(htmlfi, FileName=fi, **kwargs)
    return unicode(htmlurl)


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


def draw(g, ids=None, axishide=True, **kwargs):
    # for drawing social network
    if ids is None:
        ids = g.node.keys()
    iddic = {k: fixname(getname(k)) for k in it.ifilter(
        lambda x: x in ids, g.nodes())}
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
        g, pos,
        labels=iddic)
    if axishide:
        hideaxis(pos)

