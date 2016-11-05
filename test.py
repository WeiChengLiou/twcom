#!/usr/bin/env python
# -*- coding: utf-8 -*-

import networkx as nx
import calComNet as cn


g = nx.random_graphs.erdos_renyi_graph(1000, 0.1)
# print g.order(), g.size()

# ret = nx.closeness_centrality(g)
# print ret
# 
# ret = cn.closeness_centrality_parallel(g)
# print ret


if __name__ == '__main__':
    import timeit
    print timeit.timeit(
        "nx.closeness_centrality(g)",
        setup="from __main__ import nx, g",
        number=3,
        )
    print timeit.timeit(
        "cn.closeness_centrality_parallel(g)",
        setup="from __main__ import cn, g",
        number=3,
        )
