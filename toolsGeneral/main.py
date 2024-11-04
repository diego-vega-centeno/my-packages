import os
import json
import pathlib
from itertools import combinations


def complement(lis1, lis2):
    return set(lis1).difference(set(lis2))


def dump(path:str, data):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

    with open(path, "w", encoding='utf-8') as file:
        if(pathlib.Path(path).suffix == ".json"):  
            json.dump(data, file, indent=2)
        if(pathlib.Path(path).suffix == ".html"):  
            file.write(data)

def load(path:str):
    
    with open(path, 'r',  encoding="utf8") as fl:

        match pathlib.Path(path).suffix:
            case '.json':
                return json.load(fl)
            case '.html':
                return fl.read()

    
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

def findIntersection(lists):
    tuples = list(combinations(lists,2))
    tuplesInterBools = list(map(lambda ele : bool(intersection(ele)), tuples))
    return [x for x,y in zip(tuples, tuplesInterBools) if y == True]