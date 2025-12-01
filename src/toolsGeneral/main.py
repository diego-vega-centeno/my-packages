import os
import json
import pathlib
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import pickle
import unicodedata
import re
from collections import Counter

import json

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)

def dump(path:str, data):
    if not os.path.exists(os.path.dirname(path)) and not os.path.dirname(path) == '':
        os.makedirs(os.path.dirname(path))

    match pathlib.Path(path).suffix:
      case ".json":
        with open(path, "w", encoding='utf-8') as file:
          json.dump(data, file, indent=2, cls=SetEncoder)
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


def intersection(list1, list2) -> list:
    return list(set(list1).intersection(list2))


def camelize(name: str) -> str:
    from unidecode import unidecode

    # remove diacritics
    stringNorma = unidecode(name)
    
    stringNorma = stringNorma.split()
    stringNorma = map(lambda x: str.capitalize(str.title(x)), stringNorma)
    
    stringNorma = ''.join(stringNorma)

    return stringNorma

def count_duplicates(list):
    counts = Counter(list)
    dupes = {item: count for item, count in counts.items() if count > 1}
    return dupes

def list_diff(list1, list2):
    return [complement(list1,list2),intersection(list1,list2),complement(list2,list1)]

def deleteDuplicates(lis):

    seen = list()
    unique = []
    
    for ele in lis:
        if ele not in seen:
            unique.append(ele)
            seen.append(ele)
    
    return unique

def complement(lis1, lis2):
    
    lis2_set = set(lis2)
    return [ele for ele in lis1 if ele not in lis2_set]

def lists_Intersection(lists):
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


def normalize_country_name(name: str) -> str:
    # Remove accents
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Remove spaces and punctuation, lowercase first letter
    name = re.sub(r"[\s\W_]+", "", name)
    return name