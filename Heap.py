class Heap():

    def __init__(self):
        self.array = []#用数组形式存储堆的树结构
        self.size = 0#堆内节点个数
        self.pos = []#判断节点是否在堆中

    def newMinHeapNode(self, v, dist):
        minHeapNode = [v, dist]
        return minHeapNode

    # 交换堆中两个节点
    def swapMinHeapNode(self, a, b):
        t = self.array[a]
        self.array[a] = self.array[b]
        self.array[b] = t

    def minHeapify(self, idx):#递归，下滤根节点
        #符合完全二叉树中，索引规律
        smallest = idx
        left = 2 * idx + 1
        right = 2 * idx + 2

        if left < self.size and self.array[left][1] < \
                                self.array[smallest][1]:
            smallest = left

        if right < self.size and self.array[right][1] < \
                                self.array[smallest][1]:
            smallest = right
        #最终smallest为三个点中最小的那个的索引，非左即右

        # smallest将与左或右节点交换
        if smallest != idx:

            # Swap positions
            self.pos[ self.array[smallest][0] ] = idx
            self.pos[ self.array[idx][0] ] = smallest

            # Swap nodes
            self.swapMinHeapNode(smallest, idx)

            self.minHeapify(smallest)

    # 抽取堆中最小节点
    def extractMin(self):

        if self.isEmpty() == True:
            return

        # 找到根节点
        root = self.array[0]

        # 把最后一个节点放在根节点上去
        lastNode = self.array[self.size - 1]
        self.array[0] = lastNode

        # 更新根节点和最后一个节点的pos
        self.pos[lastNode[0]] = 0
        self.pos[root[0]] = self.size - 1#此时堆大小已经减小1

        # 减小size，从根节点开始从新构造
        self.size -= 1
        self.minHeapify(0)

        return root#返回的是被替换掉的那个

    def isEmpty(self):
        return True if self.size == 0 else False

    def decreaseKey(self, v, dist):#上滤节点

        # 获得v在堆中的位置
        i = self.pos[v]

        # 更新堆中v的距离为dist，虽说是更新，但肯定是减小key
        self.array[i][1] = dist

        # 一直寻找i的父节点，检查父节点是否更大
        while i > 0 and self.array[i][1] < self.array[int((i - 1) / 2)][1]:

            # pos数组交换，array也得交换
            self.pos[ self.array[i][0] ] = int((i-1)/2)
            self.pos[ self.array[int((i-1)/2)][0] ] = i
            self.swapMinHeapNode(i, int((i - 1)/2) )

            # i赋值为父节点索引
            i = int((i - 1) / 2)

    # 检查v是否在堆中，很巧妙的是，由于size一直在减小
    # 当pos小于size说明该点在堆中不可能的位置，即不在堆中
    def isInMinHeap(self, v):

        if self.pos[v] < self.size:
            return True
        return False