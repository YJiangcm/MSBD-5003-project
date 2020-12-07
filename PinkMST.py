import sys
from pyspark.sql.functions import pandas_udf, PandasUDFType
from Edge import Edge

from Heap import Heap


class PinkMST():
    '''
    :param
    partitionId:int
    pairedData:(list<Point>,list<Point>)
    '''

    def __init__(self,pairedData=None,partitionId=None,mstToBeMerged=None):
        self.leftData = pairedData[0]
        self.rightData = pairedData[1]
        self.partitionId = partitionId
        self.mstToBeMerged = mstToBeMerged
        if(self.rightData is None):
            self.isBipartite = False
        else:
            self.isBipartite = True


    def getMST(self):
        return self.mstToBeMerged

    def getEdgeList(self):
        if (self.isBipartite):
            edgeList = self.BipartiteMST()
        else:
            edgeList = self.PrimLocal()
        return edgeList


    def BipartiteMST(self):
        # 存每个节点的key值
        edges={}
        left_data = self.leftData
        right_data = self.rightData


        num_left = len(left_data)
        num_right = len(right_data)

        key_left = []
        key_right = []

        parent_left = []
        parent_right =[]

        # 建立最小堆
        minHeap_left = Heap()
        minHeap_right =Heap()


        # 初始化左节点三个数据结构
        for v in range(num_left):
            parent_left.append(-1)#初始时，每个节点的父节点是-1
            key_left.append(float('inf'))#初始时，每个节点的key值都是无穷大
            minHeap_left.array.append( minHeap_left.newMinHeapNode(v, key_left[v]))
            minHeap_left.pos.append(v)

        # 初始化右节点三个数据结构
        for v in range(num_right):
            parent_right.append(-1)#初始时，每个节点的父节点是-1
            key_right.append(float('inf'))#初始时，每个节点的key值都是无穷大
            minHeap_right.array.append( minHeap_right.newMinHeapNode(v, key_right[v]))
            minHeap_right.pos.append(v)

        minHeap_left.pos[0] = 0 #不懂这句，本来pos的0索引元素就是0啊
        key_left[0] = 0 #让0节点作为第一个被挑选的节点
        minHeap_left.decreaseKey(0, key_left[0])
        #把堆中0位置的key值变成key[0]，函数内部重构堆

        minHeap_right.pos[0] = 0
        #key_right[0] = 0
        minHeap_right.decreaseKey(0, key_right[0])



        # 初始化堆的大小为V即节点个数
        minHeap_left.size = num_left
        minHeap_right.size = num_right

        label ='left'
        while minHeap_left.isEmpty() == False | minHeap_right.isEmpty() == False:
            # 抽取最小堆中key值最小的节点
            if label=='left':
                newHeapNode = minHeap_left.extractMin() # 抽取堆中最小节点
                u = newHeapNode[0]

                for i in range(minHeap_right.size):
                    id= minHeap_right.array[i][0]
                    dist = max(1/42,left_data[u].dtw_dist(right_data[id]))
                    if dist < key_right[id]:
                        key_right[id] = dist
                        parent_right[id] = u

                        # 也更新最小堆中节点的key值，重构
                        minHeap_right.decreaseKey(id, key_right[id])
                        edges[(u,id)]=dist

                label='right'
            else:
                newHeapNode = minHeap_right.extractMin() # 抽取堆中最小节点
                v = newHeapNode[0]

                for i in range(minHeap_left.size):
                    id= minHeap_left.array[i][0]
                    dist = max(1/42,right_data[v].dtw_dist(left_data[id]))
                    if dist < key_left[id]:
                        key_left[id] = dist
                        parent_left[id] = v

                        # 也更新最小堆中节点的key值，重构
                        minHeap_left.decreaseKey(id, key_left[id])

                        edges[(id,v)]=dist
                label='left'


        edgePairs=[]
        for i in range(1, num_left):
            edgePairs.append((self.partitionId,Edge(right_data[parent_left[i]].getId(),left_data[i].getId(),\
                                                     edges[(i,parent_left[i])]))) #左，右，权重

        for j in range(0,num_right):
            edgePairs.append((self.partitionId,Edge(left_data[parent_right[j]].getId(),right_data[j].getId(),\
                                                    edges[(parent_right[j],j)])))


        return edgePairs




    def PrimLocal(self):
        # 存每个节点的key值
        data = self.leftData
        V = len(data)
        edges={}

        key = []
        # 记录构造的MST
        parent = []
        # 建立最小堆
        minHeap = Heap()

        # 初始化以上三个数据结构
        for v in range(V):
            parent.append(-1)#初始时，每个节点的父节点是-1
            key.append(float('inf'))#初始时，每个节点的key值都是无穷大
            minHeap.array.append( minHeap.newMinHeapNode(v, key[v]) )
            #newMinHeapNode方法返回一个list，包括节点id、节点key值
            #minHeap.array成员存储每个list，所以是二维list
            #所以初始时堆里的每个节点的key值都是无穷大
            minHeap.pos.append(v)

        minHeap.pos[0] = 0#不懂这句，本来pos的0索引元素就是0啊
        key[0] = 0#让0节点作为第一个被挑选的节点
        minHeap.decreaseKey(0, key[0])
        #把堆中0位置的key值变成key[0]，函数内部重构堆

        # 初始化堆的大小为V即节点个数
        minHeap.size = V
        # print('初始时array为',minHeap.array)
        # print('初始时pos为',minHeap.pos)
        # print('初始时size为',minHeap.size)

        while minHeap.isEmpty() == False:

            # 抽取最小堆中key值最小的节点
            newHeapNode = minHeap.extractMin()
            # print('抽取了最小元素为',newHeapNode)
            u = newHeapNode[0]

            for i in range(minHeap.size):
                id= minHeap.array[i][0]
                dist = max(1/42,data[u].dtw_dist(data[id]))
                if dist < key[id]:
                    key[id] = dist
                    parent[id] = u

                    # 也更新最小堆中节点的key值，重构
                    minHeap.decreaseKey(id, key[id])
                    edges[(u,id)]=dist
                    edges[(id,u)]=dist

        edgePairs=[]
        for i in range(1, V):
            edgePairs.append((self.partitionId,Edge(data[parent[i]].getId(),data[i].getId(),edges[(parent[i],i)])))

        return edgePairs

    @staticmethod
    def displayValue(leftData,rightData,numPoints):
        for i in range(numPoints):
            print("==== samples " , i , ", " , leftData[i] ," || "
                    , rightData[i] if rightData != None else None)