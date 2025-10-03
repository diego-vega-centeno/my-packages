import os
import json
import pathlib
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import pickle

def dump(path:str, data):
    if not os.path.exists(os.path.dirname(path)) and not os.path.dirname(path) == '':
        os.makedirs(os.path.dirname(path))

    match pathlib.Path(path).suffix:
      case ".json":
        with open(path, "w", encoding='utf-8') as file:
          json.dump(data, file, indent=2)
      case ".html":
        with open(path, "w", encoding='utf-8') as file:
          file.write(data)
      case ".pkl":
        with open(path, 'wb') as file:
          pickle.dump(data, file)


def load(path:str):
    

  match pathlib.Path(path).suffix:
    case '.json':
      with open(path, 'r',  encoding="utf8") as file:
        return json.load(file)
    case '.html':
      with open(path, 'r',  encoding="utf8") as file:
        return file.read()
    case '.pkl':
      with open(path, 'rb') as file:
        return pickle.load(file)

    
def sortDictKeys(dict, keys):
    return {k:dict[k] for k in sorted(keys, key=lambda x:keys.index(x))}


def insertKeyDict(inDict, pair, key):
    res = dict()
    for k in inDict:
        res[k] = inDict[k]
        if k == key:
            res.update(pair)
    # inDict = res
    return res

def dictFilterKeys(inDict, keys):
    return dict(filter(lambda pair : pair[0] in keys, inDict.items()))

def dictRemoveKeys(inDict, keys):
    return dict(filter(lambda pair : pair[0] not in keys, inDict.items()))


def intersection(pair) -> list:
    return list(set(pair[0]).intersection(pair[1]))


def camelize(name: str) -> str:
    from unidecode import unidecode

    # remove diacritics
    stringNorma = unidecode(name)
    
    stringNorma = stringNorma.split()
    stringNorma = map(lambda x: str.capitalize(str.title(x)), stringNorma)
    
    stringNorma = ''.join(stringNorma)

    return stringNorma

def findDuplicates(list):
    seen = []
    dup = []
    for id in list:
        if id in seen:
            dup.append(id)
        seen.append(id)
    
    return dup

def deleteDuplicates(lis):

    seen = list()
    unique = []
    
    for ele in lis:
        if ele not in seen:
            unique.append(ele)
            seen.append(ele)
    
    return unique

def complement(lis1, lis2):
    
    comple = []
    for ele in lis1:
        if ele not in lis2:
            comple.append(ele)

    return comple

def findIntersection(lists):
    tuples = list(combinations(lists,2))
    tuplesInterBools = list(map(lambda ele : bool(intersection(ele)), tuples))
    return [x for x,y in zip(tuples, tuplesInterBools) if y == True]


def getFirst(dictio, options):
    return next((dictio[key] for key in options if key in dictio), None)

def tryFunction(function, arg, timeout=60):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(function, arg)
        try:
            print(f"processing: {arg} ...")
            result = future.result(timeout=timeout)
            print("finished: success")
            return result # Set a timeout for each call
        except TimeoutError:
            print(f"finished: timeout")
            return 0