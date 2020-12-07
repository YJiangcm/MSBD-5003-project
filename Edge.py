class Edge(object):
    def __init__(self, end1, end2, weight):
        self.left = end1
        self.right = end2
        self.weight = weight

    def getLeft(self):
        return self.left

    def getRight(self):
        return self.right

    def getWeight(self):
        return self.weight

    def toString(self):
        return "left : %d right:%d "