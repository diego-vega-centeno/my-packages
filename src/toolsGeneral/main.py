import os
import json
import pathlib
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import pickle
import unicodedata
import re
from collections import Counter
from pathlib import Path

import json

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return {"type": "set", "items": sorted(list(obj))}
        return super().default(obj)

def decode_sets(obj):
    if "type" in obj and obj["type"] == "set":
        return set(obj["items"])
    return obj

def transform(obj):
    if isinstance(obj, set):
        items = [transform(x) for x in obj]
        items.sort(key=json.dumps)
        return {"type": "set", "items": items}
    if isinstance(obj, tuple):
        return {"type":"tuple", "items":[transform(x) for x in obj]}
    if isinstance(obj, list):
        return [transform(x) for x in obj]
    if isinstance(obj, dict):
        return {k: transform(v) for k, v in obj.items()}
    return obj

def untransform(obj):
    if isinstance(obj, dict) and obj.get("type") == "set":
        return set(untransform(x) for x in obj["items"])
    if isinstance(obj, dict) and obj.get("type") == "tuple":
        return tuple(untransform(x) for x in obj["items"])
    if isinstance(obj, dict):
        return {k: untransform(v) for k, v in obj.items()}
    # Will need to use this in cases a dict is made from an unordered source
    # e.g.
        # keys = {"a", "b", "c"}  # a set
        # d = {k: k.upper() for k in keys}
    # if isinstance(obj, dict):
    #     return {k: transform(obj[k]) for k in sorted(obj)}
    if isinstance(obj, list):
        return [untransform(x) for x in obj]
    return obj

def dump(path:str, data):
    _dir = os.path.dirname(path)
    if _dir:
        os.makedirs(_dir, exist_ok=True)

    match pathlib.Path(path).suffix:
        case ".json":
            with open(path, "w", encoding='utf-8') as file:
                # use a manual transform instead of the subclass SetEncoder
                # there's no way to override the list object conversion.
                # For example for: [(a,b,c),(d,e,f)]
            #   return json.dump(data, file, indent=2, cls=SetEncoder)
                json.dump(transform(data), file, indent=2)
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
        return untransform(json.load(file))
        # return json.load(file, object_hook=decode_sets)
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

def find_duplicates(list):
    counts = count_duplicates(list)
    return [item for item, count in counts.items() if count > 1]

def tally(list):
    return Counter(list)

def list_diff(list1, list2):
    return [complement(list1,list2),intersection(list1,list2),complement(list2,list1)]

def delete_duplicates(lis):

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

def load_cleaned_dirs(dir:Path, countries=None, extension='*'):
    dirs = [dir for dir in dir.glob("*") if dir.is_dir()]
    if countries:
        dirs = [dir for dir in dirs if dir.name in countries]
    loaded_data = {}
    for dir in dirs:
        loaded_data[dir.name] = {}
        files = [f for f in dir.glob(f"*.{extension}") if f.is_file()]
        for f in  files:
            data = load(f)
            loaded_data[dir.name].update(data)
    return loaded_data

def load_dirs(dir:Path, countries=None, extension='*'):
    loaded_data = {}
    dirs = [dir for dir in dir.glob("*") if dir.is_dir()]
    if countries:
        dirs = [dir for dir in dirs if dir.name in countries]
    for dir in dirs:
        try:
            files = [f for f in dir.glob(f"*.{extension}") if f.is_file()]
            if len(files) > 1:
                loaded_data[dir.name] = {}
                for f in files:
                    loaded_data[dir.name].update(load(f))
            else:
                loaded_data[dir.name] = load(files[0])
        except:
            print(f"Error in dir: {dir}")
        
    return loaded_data