class dData(dict):
    def __new__(cls, base = None):
        # print(cls)
        # print(base)
        if isinstance(base, dict):
            return super().__new__(cls)
        return base

    def cleanProps(self):
        if isinstance(self, dData):
            temp = {k:(dData(v).cleanProps() if isinstance(v, dict) else v) for k,v in self.items()}
            return dData({k:v for k,v in temp.items() if v != {}})

    # def __repr__(self):
    #     return f'dData({super().__repr__()})'

    def filterKey(self, key):
        if key[0] == "all":
            return dData(self)
        elif isinstance(key, list):
            return dData({k:v for k, v in self.items() if k in key})
        else:
            return dData({k:v for k, v in self.items() if k == key})
    
    def filter(self, *keys):
        if len(keys) == 1:
            return dData(self.filterKey(keys[0]))
        else:
            firstKeyData = self.filterKey(keys[0])
            return dData({k : dData(v).filter(*keys[1:]) for k, v in firstKeyData.items()})
  