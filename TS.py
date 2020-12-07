from  pyspark.sql.types import DataType
from  pyspark.sql.types import UserDefinedType
class TS(object):
    def __init__(self):
        self.id = 0
        self.ts = []

    def getId(self):
        return self.id

    def getTs(self):
        return self.ts

    def setId(self,id):
        self.id = id
        return self

    def setValue(self,v):
        self.ts=v
        return self