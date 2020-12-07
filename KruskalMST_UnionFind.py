from collections import defaultdict
from Edge import Edge
from Heap import Heap


class Graph:

    def __init__(self,node,graph):
        self.graph = graph  # 二维list用来存边的起点、终点、权重
        self.node =node

    # 添加每条边
    def addEdge(self, u, v, w):
        if u not in self.node:
            self.node.append(u)
        if v not in self.node:
            self.node.append(v)
        self.graph.append([self.node.index(u), self.node.index(v), w])

    # 递归找到每个节点所在子树的根节点
    def find(self, parent, i):
        if parent[i] == i:
            return i
        return self.find(parent, parent[i])

    # 联合两颗子树为一颗子树，谁附在谁身上的依据是rank
    def union(self, parent, rank, x, y, xroot, yroot):
        # xroot = self.find(parent, x)不需要重复此函数
        # yroot = self.find(parent, y)不需要重复此函数

        # 进行路径压缩
        if (xroot != parent[x]):
            parent[x] = xroot
        if (yroot != parent[y]):
            parent[y] = yroot

            # Attach smaller rank tree under root of high rank tree (Union by Rank)
        if rank[xroot] < rank[yroot]:
            parent[xroot] = yroot
        elif rank[xroot] > rank[yroot]:
            parent[yroot] = xroot

        # If ranks are same, then make one as root and increment its rank by one
        else:
            parent[yroot] = xroot
            rank[xroot] += 1

    # 主函数用来构造最小生成树
    def KruskalMST(self):
        V=len(self.node)
        result = []  # 存MST的每条边

        i = 0  # 用来遍历原图中的每条边，但一般情况都遍历不完
        e = 0  # 用来判断当前最小生成树的边数是否已经等于V-1

        # 按照权重对每条边进行排序，如果不能改变给的图，那么就创建一个副本，内建函数sorted返回的是一个副本
        self.graph = sorted(self.graph, key=lambda item: item[2])

        parent = []
        rank = []

        # 创建V个子树，都只包含一个节点
        for node in range(V):
            parent.append(node)
            rank.append(0)

        # MST的最终边数将为V-1
        while (e < V - 1):
            if i>len(self.graph)-1:
                break

            # 选择权值最小的边，这里已经排好序
            u, v, w = self.graph[i]
            i = i + 1
            x = self.find(parent, u)
            y = self.find(parent, v)

            # 如果没形成边，则记录下来这条边
            if x != y:
                # 不等于才代表没有环
                e = e + 1
                result.append(Edge(self.node[u], self.node[v], w))
                self.union(parent, rank,u, v, x, y)
            # 否则就抛弃这条边

        return result

    def KruskalMST_heap(self):
        V=len(self.node)
        result = []  # 存MST的每条边
        E=len(self.graph)
        i = 0  # 用来遍历原图中的每条边，但一般情况都遍历不完
        e = 0  # 用来判断当前最小生成树的边数是否已经等于V-1

        # 按照权重对每条边进行排序，如果不能改变给的图，那么就创建一个副本，内建函数sorted返回的是一个副本
        # self.graph = sorted(self.graph, key=lambda item: item[2])

        #初始化最小堆
        minHeap = Heap()
        for i in range(len(self.graph)):
            edge = self.graph[i]
            minHeap.array.append(minHeap.newMinHeapNode(v=i,dist=edge[2]))
            #newMinHeapNode方法返回一个list，包括节点id、节点key值
            #minHeap.array成员存储每个list，所以是二维list
            #所以初始时堆里的每个节点的key值都是无穷大
            minHeap.pos.append(i)

        minHeap.size = E
        minHeap.decreaseKey(0,minHeap.array[0][1])

        parent = []
        rank = []

        # 创建V个子树，都只包含一个节点
        for node in range(V):
            parent.append(node)
            rank.append(0)

        # MST的最终边数将为V-1
        while (e < V - 1):
            if i>len(self.graph)-1:
                break

            # 选择权值最小的边，这里已经排好序
            edge = minHeap.extractMin()
            index = edge[0]
            dist = edge[1]
            minHeap.decreaseKey(index,dist)
            u,v,w = self.graph[index]
            i = i + 1
            x = self.find(parent, u)
            y = self.find(parent, v)

            # 如果没形成边，则记录下来这条边
            if x != y:
                # 不等于才代表没有环
                e = e + 1
                result.append(Edge(self.node[u], self.node[v], w))
                #result.append([u,v,w])
                self.union(parent, rank,u, v, x, y)
            # 否则就抛弃这条边

        return result

def printEdge(edges):
    for edge in edges:
        print("(",edge.getLeft(),",",edge.getRight(),",",edge.getWeight(),")")

def kruskalReducer(leftEdges, rightEdges):
    nodes=[]
    graph=[]
    edges = leftEdges+rightEdges
    for edge in edges:
        u=edge.getLeft()
        v=edge.getRight()
        if u not in nodes:
            nodes.append(u)
        if v not in nodes:
            nodes.append(v)
        w=edge.getWeight()
        graph.append([nodes.index(u),nodes.index(v),w])
    g = Graph(graph=graph,node=nodes)
    return g.KruskalMST()



