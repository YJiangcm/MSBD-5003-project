from pyecharts import Graph
import sys


def graph_show(edge_list):
    nodes=[]
    links=[]
    nodes_list=[]
    w_min=sys.float_info.max
    w_max=0
    for edge in edge_list:
        u, v, w = edge.getLeft(), edge.getRight(), edge.getWeight()

        if w<w_min:
            w_min=w
        if w>w_max:
            w_max=w

        if u not in nodes_list:
            nodes_list.append(u)
            nodes.append({"name": str(u), "symbolSize": 10})

        if v not in nodes_list:
            nodes_list.append(v)
            nodes.append({"name": str(v), "symbolSize": 10})

        links.append({"source": str(u), "target": str(v), "value": w})

    print("min w:",w_min)
    print("max w:",w_max)
    graph = Graph("Minimum Spanning Tree")
    graph.add("", nodes, links, graph_layout='force',graph_edge_length=[w_min,w_max],graph_gravity =0,graph_repulsion=0,is_label_show=True)
    graph.show_config()
    graph.render()



import networkx as nx
import matplotlib.pyplot as plt
def graph_show_networkx(edge_list):
    nodes=[]
    G = nx.Graph()
    for edge in edge_list:
        u, v, w = edge.getLeft(), edge.getRight(), edge.getWeight()
        if u not in nodes:
            nodes.append(u)
        if v not in nodes:
            nodes.append(v)
        G.add_edge(u,v, length = w/10)

    pos = nx.spring_layout(G)
    nx.draw(G, pos)
    nx.draw_networkx_edge_labels(G, pos)


    plt.savefig("pic.png")
    plt.show()