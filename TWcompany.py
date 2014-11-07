# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

output_path='../repository'
urlpath='http://weichengliou.github.io/repository'
output_path='output'
urlpath=output_path

from os.path import join
import matplotlib as mpl
from matplotlib import pylab as plt
import pandas as pd
import numpy as np
from collections import defaultdict
import itertools as it
from datetime import datetime
import networkx as nx
font='AR PL KaitiM Big5'
mpl.rcParams['font.sans-serif'] = font

from IPython.display import HTML
from markdown import markdown
import csv
#from mpld3 import enable_notebook, disable_notebook
#enable_notebook()
#mpl.rcParams['figure.figsize'] = 12, 9

import seaborn as sns
sns.set(font=font)
sns.set_context('poster')
#size_poster = mpl.rcParams['figure.figsize']
mpl.rcParams['figure.figsize'] = 16, 12

def rendermd(markdown_str):
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
    return s.replace(u'<table>',u'<table border="{0}">'.format(border))

def html_template(htmlstr,**kwargs):
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
</html>""".format(u''.join(map(unicode,htmlstr)))

def writehtml(li, fi):
    with open(fi, 'wb') as f:
        f.write(html_template(ListTable(li)._repr_html_()).encode('utf8'))
    
def writecsv(li, fi):
    with open(fi, 'wb') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',',
                               quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for r in li:
            csvwriter.writerow(map(lambda x:unicode(x).encode('utf8'),r))

def htmlcsv(msg, df, fi):
    html_url = u"{0}/html/{1}.html".format(output_path, fi)
    csv_url = u"{0}/csv/{1}.csv".format(output_path, fi)
    writehtml(df, html_url)
    writecsv(df, csv_url)
    markdown_str=u"{0}：[HTML]({1}), [CSV]({2})".format(msg, html_url, csv_url)
    return HTML(u"{}</p>".format(markdown(markdown_str)))

def write_d3(fi, dir, **kwargs):
    """
    Export d3 file, return htmrul
    """
    fi1=fi.replace('json', 'html')
    htmlfi = os.path.join(output_path, dir, fi1)
    htmlurl = os.path.join(urlpath, dir, fi1)

    render_html(htmlfi, FileName=fi, **kwargs)
    return unicode(htmlurl)

# <codecell>

def hideaxis(pos=None):
    if pos:
        df=pd.DataFrame(pos.values(), columns=['x','y'])
        plt.xlim([df['x'].min()-5, df['x'].max()+5])
        plt.ylim([df['y'].min()-5, df['y'].max()+5])
    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())

    
def getcentral(g1):
    return pd.DataFrame({
      u'anc': {x:len(nx.ancestors(g1, x)) for x in g1.nodes()},
      u'des': {x:len(nx.descendants(g1, x)) for x in g1.nodes()},
      u'indeg': g1.in_degree(),
      u'outdeg': g1.out_degree()
      })


def printdf(s, coldic=None):
    x = ListTable()
    cols = []
    for k in s.columns:
        if coldic and k in coldic:
            cols.append(coldic[k])
        else:
            cols.append(k)            
    x.append(cols)
            
    for k, r in s.iterrows():
        x.append(r.values)
    return x

def defaultsize():
    disable_notebook()
    mpl.rcParams['figure.figsize'] = 12.8, 8.8
#enable_notebook()

def draw(g, ids, axishide=True, **kwargs):
    iddic = {k:fixname(getname(k)) for k in it.ifilter(lambda x:x in ids, g.nodes())}
    node_color = map(lambda k:k[1].get('group', 0), g.node.iteritems())
    pos = nx.graphviz_layout(g)
    vmax = kwargs.get('vmax', max(node_color))
    node_size = kwargs.get('node_size', 40)
    cmap = kwargs.get('cmap', plt.cm.jet)
    
    nx.draw_networkx_edges(g, pos,
                     with_labels=False,
                     alpha=0.5,
                     edge_width=0.5,
                     edge_color='y',
                     )
    nx.draw_networkx_nodes(g, pos,
                     alpha=0.5,
                     node_color=node_color,
                     vmin=0,
                     vmax=vmax,
                     cmap=cmap,
                     node_size=node_size,
                     )
    nx.draw_networkx_labels(g, pos,
                            labels=iddic)
    if axishide:
        hideaxis(pos)
       

# <codecell>

from twcom.query import *
from twcom.utils import *
from visualize.output import *
cn=init()

# <markdowncell>

# ## 從董監事名單觀察法人投資關係
# 
# 台灣的企業種類分佈相當廣泛，有政府經營的國營企業、廣大的中小企業，家族企業為背景的財團，還有黨營事業，這些不同的市場參與者組成了現今的企業生態圈。但對於檯面上的企業財團之間存在怎樣的競合關係，我們卻始終無法一覽全貌。近年來社會網絡分析 (Social Network Analysis）相當盛行，藉著觀察法人投資關係我們可以更清楚政府、財團甚至黨營事業組成一個大群體。這群體的成員組成有誰？他們的規模究竟有多大？該如何界定他們的影響力？這些都將在下文逐一分析討論。

# <codecell>

#cmd="select count(distinct comdst_id) from comivst"
#val1, = cn.execute(cmd).fetchone()
#cmd="select count(distinct comsrc_id) from comivst"
#val2, = cn.execute(cmd).fetchone()

badstate = list(badstatus(cn))
totcnt = cn.cominfo.find().count()
localcnt = cn.cominfo.find({'type':{'$in': ['baseinfo', 'fbranchinfo', 'fagentinfo']}, 'status':{'$nin': badstate}}).count()
val1 = cn.comivst.find().distinct('dst')
val2 = cn.comivst.find().distinct('src')
val12 = len(set(val1).union(set(val2)))

val3 = float(val12)/(localcnt)*100

#val4 = float(freq[0])/sum(freq)*100

markdown_str=[u'本研究資料來源為 [台灣公司關係圖 http://gcis.nat.g0v.tw/](http://gcis.nat.g0v.tw/) 7 月份資料，',
              u'整體樣本數為 {0:,d} 家公司。'.format(totcnt),
              u'由於資料來源並不含時間序列的變數，無法觀察不同時點的公司存活家數變化，故僅考慮營運中的企業。',
              u'台灣整體營運中的企業數 (不含分公司) 約 {0:,d} 家。'.format(localcnt),
              u'其中有董監事名單或被列入董監事名單的公司約有 {0:,d} 家，'.format(val12),
              u'這些約佔整體營運中的企業 %1.2f%% 左右。' % (val3),
              u'我們想了解的，就是這些公司為台灣形塑出來的企業生態圈概貌。']

rendermd(markdown_str)
#HTML(u"<p>{}</p>".format(markdown(u''.join(markdown_str))))

# <markdowncell>

# 本報告將依以下順序進行探討：
# 
# * 樣本組成概貌
# * 台灣整體企業網絡成份
# * 誰是老大？誰是幕後藏鏡人？
# * 企業影響力排名

# <markdowncell>

# ## 台灣企業網絡生態圈

# <codecell>

execfile('temp.py')
gall = getcomgraph()
#gs = [g for g in nx.weakly_connected_component_subgraphs(gall)]

# <codecell>

#gs = map(len, nx.weakly_connected_components(gall))
#cPickle.dump(gs, gzip.open('weakly_comp.dat','wb'), True)

gs = cPickle.load(gzip.open('weakly_comp.dat'))

# <codecell>

g2 = nx.DiGraph()
for i, g in enumerate(gs):
    if g>3:
        g2.add_node(i, {'size': g})
pos = nx.graphviz_layout(g2)

# <codecell>

nx.draw_networkx_nodes(g2, pos, 
                       with_labels=False,
                       alpha=0.5,
                       node_size=[max(10,g['size']) for g in g2.node.values()],
                       )
hideaxis(pos)

# <markdowncell>

# 根據不同企業間的法人投資關係，可將企業們分成大小不同的群體，進而繪出如上圖的台灣企業投資網路分佈（圓點大小代表企業數量多寡）。為簡化圖形複雜度，在此僅列出三家以上公司形成的群體。圖中多數群體的規模（此處群體規模為群體內的企業家數）相近，但有一個群體規模明顯大於其他群體。這群體究竟有多大？他的群體規模是第二名的三百倍以上。
# 
# 這規模最大的群體，其交叉投資關係可繪成以下社會網路圖：

# <codecell>

execfile('temp.py')
#g1 = nx.weakly_connected_component_subgraphs(gall)[0]
#pos1 = nx.graphviz_layout(g1)
#cPickle.dump((g1, pos1), gzip.open('comall.dat', 'wb'), True)

g1, pos1 = cPickle.load(gzip.open('comall.dat'))
drawall(g1, pos1)

# <markdowncell>

# 上圖中紅點部份為該群體中直接投資公司數量前 25 名的公司。這些公司成員有官股行庫、財團與黨營事業。這些公司正位於群體的核心附近，透過子公司繼續投資其他公司形成間接投資。其他也有財團間透過策略聯盟交叉投資，或是共同投資新事業結合成利益共同體。種種複雜的投資關係下建構出了一個超巨大關係企業集團。對於台灣大型企業集團的結構與形成過程，可參考中研院李宗榮老師發表的研究文章 [1](http://newsletter.sinica.edu.tw/file/file/36/3602.pdf), [2](http://sociology.ntu.edu.tw/ntusocial/journal/ts-13/1-4all.pdf)。

# <markdowncell>

# 對於群體規模與相同規模的群體個數之間的關係，我們可繪成散佈圖如下：

# <codecell>

s = pd.Series(gs).value_counts()
s1=s[s.index != 1]
plt.plot(s1.index, s1.values, '.', ms=20)
plt.xscale('log')
plt.yscale('log')
plt.title(u'群體個數/群體規模散佈圖')
plt.xlabel(u'群體規模 (log)')
_=plt.ylabel(u'群體個數 (log)')

# <markdowncell>

# 由圖可觀察到一些現象：
# 
# * 絕大多數公司僅存在少數法人投資關係。
# * 規模越大的群體，其個數與群體規模成指數遞減的關係。
# * 規模最大的群體有一萬個以上成員。
# 
# 這些由法人投資而形成的群體可視為具有廣義上的集團關係。集團所帶來的好處，不僅是投資方提供資金，取得被投資方的經營利潤而已。這當中還帶來了更多的資訊、人脈等無形優勢，幫助集團成員攫取更多利益。而集團的存在對經濟發展亦有指標性的影響，因為集團本身的規模優勢與市場地位而築成強勢的議價力量，會成為其他企業營運上不可忽視的因素之一。

# <codecell>


#dfivst = getcentral(gall)
#cPickle.dump(dfivst, gzip.open('central_all.dat', 'wb'), True)

dfivst = cPickle.load(gzip.open('central_all.dat'))
dfivst.sort(u'outdeg', ascending=False, inplace=True)
dfivst[u'name'] = [''] * len(dfivst)
dfivst=dfivst[['name','anc','des','indeg','outdeg']]
coldic={'rank': u'排名',
        'name': u'單位名稱', 
        'anc': u'祖先數',
        'des': u'子孫數',
        'indeg': u'直接被投資',
        'outdeg': u'直接投資'}

# <markdowncell>

# ## 誰是老大哥？誰是藏鏡人？
# 
# 在觀察到台灣存在這麼一個超大關係企業集團後，令人忍不住想了解其中的組成成份。
# 
# 在此我們試從兩個面向探討：直接投資家數、直接投資加上間接投資家數，來觀察誰是當中的老大哥。
# 
# ### 直接投資家數排名
# 
# 整體而言，最多直接投資的公司有誰呢？
# 根據全體法人投資關係，列出前 25 大公司如下：

# <codecell>

dfivst.sort(u'outdeg', ascending=False, inplace=True)
#dfivst=dfivst[['name','anc','des','indeg','outdeg']]
s = dfivst.head(500).copy()
s['rank'] = range(1, 1+len(s))
s['name'] = map(getname, s.index)
s['inbig1'] = [('O' if x in g1 else 'X') for x in s.index]
s=s[['rank','name','anc','des','indeg','outdeg','inbig1']]
coldic['inbig1'] = u'是否身處最大集團'
df=printdf(s[:25], coldic)
df

# <codecell>

df=printdf(s, coldic)
htmlcsv(u'前五百大詳細內容見此', df, fi='outdeg_rank')

# <markdowncell>

# 以上可觀察到前 25 名企業中，僅有兩間公司不屬於最大集團的一份子。事實上即使前 100 名中也僅有五間公司不屬於最大集團的一份子。（見下表）

# <codecell>

li = ListTable()
li.append([u'名次', u'名稱', u'直接投資'])
for i, k in enumerate(dfivst.index[:100]):
    if k not in g1.node:
        li.append([i, getname(k), dfivst.ix[k, u'outdeg']])
        
li

# <markdowncell>

# 觀察前 25 名可以看到組成份子可略分三大類：財團、官股、黨營事業。前兩名的兆豐國際商業銀行與中華開發工業銀行，因過去的工業銀行背景，故至今仍佔有不少公司的董監事席次。中華開發昔日具備官股與黨營事業背景，但時至今日已無黨營事業席次，而官股亦已僅剩一席。
# 
# 國民黨麾下的中央投資，與大同公司、太陽光電能源科技公司同時列名第 21 名。自過去一黨專政時代開始，國民黨即擁有不少資產。雖然社會自過去一直不斷有要求國民黨歸還黨產的聲音出現，但至今仍有堪與其他財團相比擬的轉投資公司家數。像國民黨這樣具有財團實力的政黨在世界各國當中已是少數，其身處台灣最大關係企業集團中的一員，更顯示了其與財團之間利害關係密切。
# 
# 對於直接投資前 50 名的公司，可繪出一階社會網路圖如下：

# <codecell>

dfivst = dfivst.sort(columns='outdeg', ascending=False)
ids = dfivst.index[:50].tolist()
g = get_network(ids, 1)
for k, v in g.node.iteritems():
    if k in ids:
        v['group'] = 1
    else:
        v['group'] = 0
        
draw(g, ids)

fi = 'outdeg_network.lv1.json'
exp_company(g, jsonfi=join(output_path, 'html', fi))
html_url=write_d3(fi, 'html', color='category10')

markdown_str=u"[另開新頁]({0})".format(unicode(html_url))
HTML(markdown(markdown_str)) #+\
#     u'<iframe height="600px" width="100%" src={0}></iframe>'.format(html_url))

# <markdowncell>

# 僅從一階社會網路圖來看，這些原本即身處最大關係企業集團之內的企業已形成一個群體，表示這些企業間的距離在兩度 (degree <= 2) 之內，足證這些企業間的關係密切。

# <markdowncell>

# ### 直接投資＋間接投資排名
# 
# 接下來試圖從直接投資＋間接投資（簡稱為子孫數）排名觀察。母公司投資子公司，子公司又投資孫公司，如此層層相連形成一廣大關係企業結構。雖說最上層公司未必能影響最下層公司，但仍可視為有間接影響力。根據全體法人投資關係，列出子孫數前 25 大公司如下：

# <codecell>

dfivst.sort(u'des', ascending=False, inplace=True)
#dfivst=dfivst[['name','anc','des','indeg','outdeg']]
s = dfivst.head(500).copy()
s['rank'] = range(len(s))
s['name'] = map(getname, s.index)
s['inbig1'] = [('O' if x in g1 else 'X') for x in s.index]
s=s[['rank','name','anc','des','indeg','outdeg','inbig1']]
coldic['inbig1'] = u'是否身處最大集團'
df=printdf(s[:25], coldic)
df

# <codecell>

df=printdf(s, coldic)
htmlcsv(u'前五百大詳細內容見此', df, fi='des_rank')

# <markdowncell>

# 從子孫數排名有幾項觀察：
# 
# 以財政部為首，影響最多公司（台灣金控與台灣銀行為官股行庫成員，由財政部直接投資），其亦身處最大集團之內。雖說此亦有公私利益混雜的疑慮，但與黨營事業的差別在於：政府由全體人民投票決定，有可能政黨輪替，但是政黨則否。
# 
# 而國發基金的成立宗旨在於改善產業結構、投資協助產業創新，其影響力排名第二。惟其評估標的方式與是否有受到有效監管則不在本文討論範圍之內。
# 
# 除官股行庫與國發基金以外，其餘公司多以投資公司為主。此因現今經營控制財團企業的家族們，多以投資公司的形式控制企業。其直接投資家數多在十家以下，堪稱為財團幕後的藏鏡人。值得注意的是，榜上排名前 25 名的公司中，有大半屬於泛新光集團（新光＋台新）的一份子，新光集團旗下成員眾多，大股東透過多家投資公司交叉持有旗下企業，故這些公司的子孫數中有大量重複對象。
# 
# 關於這些企業間的關係，可以根據前 50 名企業繪製一階企業社會網路圖如下：

# <codecell>

dfivst = dfivst.sort(columns='des', ascending=False)
ids = dfivst.index[:50].tolist()
g = get_network(ids, 1)
for k, v in g.node.iteritems():
    if k in ids:
        v['group'] = 1
    else:
        v['group'] = 0
        
draw(g, ids)

fi = 'des_network.json'
exp_company(g, jsonfi=join(output_path, 'html', fi))
html_url=write_d3(fi, 'html', color='category10')

markdown_str=u"[另開新頁]({0})".format(unicode(html_url))
HTML(markdown(markdown_str)) #+\
#     u'<iframe height="600px" width="100%" src={0}></iframe>'.format(html_url))

# <codecell>

g2 = get_network(ids, maxlvl=3)
n2 = len(g2.node)
msg = [u"從上圖可以看到，前 25 名的企業大致可分為兩大群體：官股與泛新光集團。",
       u"一階的企業網絡圖共有 {0} 個點，佔不到該最大群體的 10％。".format(len(g.node)),
       u"若是延伸至三階共有 {0} 個點，共佔該最大群體 {1:1.2f} %。".format(n2, float(n2)/len(g1.node)*100)]
HTML(markdown(u''.join(msg)))

# <markdowncell>

# 但是這種觀察方式僅能觀察得到官股與新光集團影響力龐大的結論，那麼其他家族與企業集團又如何呢？接下來將試圖應用社會網路分析的部份概念，來協助找出居於群體核心位置的企業。

# <markdowncell>

# ### 中間度排名
# 
# 這部份應用近距中間度指標 (closeness centrality) 試圖找出居於核心位置的企業。先暫列排名前 25 名企業如下：

# <codecell>

#s1=nx.betweenness_centrality(g1)
s2=nx.closeness_centrality(g1)
#dfivst['betwu'] = pd.Series(s1)
dfivst['closu'] = pd.Series(s2)

# <codecell>

s = dfivst.fillna(0).sort(columns='closu', ascending=False).head(500).copy()
s['rank'] = range(1, 1+len(s))
s['name'] = map(getname, s.index)
s['inbig1'] = [('O' if x in g1 else 'X') for x in s.index]
s=s[['rank','name','anc','des','indeg','outdeg','inbig1']]
coldic['inbig1'] = u'是否身處最大集團'
df=printdf(s[:25], coldic)
df

# <codecell>

df=printdf(s, coldic)
htmlcsv(u'前五百大詳細內容見此', df, fi='betw_rank')

# <markdowncell>

# 排名前 25 名的企業，多半以官股行庫、國發基金與國營事業居多。其次為泛新光集團、統一集團與裕隆集團。
# 
# 若是觀察前一百名企業的一階企業網路圖（見下圖），可看到多數企業集合成一個群體，表示這些企業間的距離大多在兩度以內，再次印證了這些企業間關係密切程度。其中新光集團旗下企業佔了不少，可見其企業經營網絡之廣闊。

# <codecell>

ids = s.index[:100].tolist()
g = get_network(ids, 1)
for k, v in g.node.iteritems():
    if k in ids:
        v['group'] = 1
    else:
        v['group'] = 0
        
draw(g, ids)

fi = 'clos_network.lv1.json'
exp_company(g, jsonfi=join(output_path, 'html', fi))
html_url=write_d3(fi, 'html', color='category10')

markdown_str=u"[另開新頁]({0})".format(unicode(html_url))
HTML(markdown(markdown_str)) #+\
#     u'<iframe height="600px" width="100%" src={0}></iframe>'.format(html_url))

# <markdowncell>

# ## 結語
# 
# 本文試從社會網路分析角度揭示台灣經濟發展現況。從整體觀之，台灣存在一個由政府、財團與黨營事業組成之超巨大關係企業集團。關於其形成方式自有其歷史背景，並非本文探討重點。然吾人必須了解，僅僅從法人投資而言，財團間的利益關係遠比我們想像的還要密切。更遑論因血親、姻親而組成的親族關係，將使這集團規模進一步擴大。
# 
# 另外我們也發現黨營事業亦身處這個巨大利益共同體裡面，這點在政黨決策方向的制定與執行上，將不可避免地造成巨大影響。如何有效監督與減少政黨與財團間的裙帶關係影響，仍有待全民共同努力。
# 
# 本文研究僅為一個起點，後續將有更多的深入研究，協助我們更了解台灣的企業生態環境。
# 
# ## 參考文獻
# 
# 1. [李宗榮，〈台灣企業間的親屬網絡〉，《中央研究院週報》 No. 1216](http://newsletter.sinica.edu.tw/file/file/36/3602.pdf)
# 2. [李宗榮，2007，〈在國家與家族之間：企業控制與臺灣大型企業間網絡再探〉，《台灣社會學》，第13卷，頁173-242。(TSSCI)](http://sociology.ntu.edu.tw/ntusocial/journal/ts-13/1-4all.pdf)

# <codecell>

#HTML(hidecode)

