import requests
import os
import json

def getOSMIDAddsStruct(relId: str, lvls: list):

    endPoint = "http://overpass-api.de/api/interpreter"

    query = f"""
        [timeout:3600][out:json];

        rel({relId});
        out tags;
        map_to_area;
        rel[boundary=administrative][admin_level={lvls[0]}](area);
        out tags;

        foreach{{
            ._->.elem;
            map_to_area;
            rel[boundary=administrative][admin_level={lvls[1]}](area)->.elemSubs;
            (.elemSubs;.elemAllSubs;)->.elemAllSubs;
            .elemSubs;
            convert rel ::id = id(), ::=::, parent_id=elem.set(id());
            out tags;
        }};
        .elemAllSubs;
        foreach{{
            ._->.elem;
            map_to_area;
            rel[boundary=administrative][admin_level={lvls[2]}](area)->.elemSubs;
            .elemSubs;
            convert rel ::id = id(), ::=::, parent_id=elem.set(id());
            out tags;
        }};
    """
    res = requests.get(endPoint, params={'data': query})

    return res.json()


# def makeAddsTree(idList, dataIndex):
#     return {id: {**dataIndex[id], "childs":(makeAddsTree(relsDataIndex[id]["childs"], dataIndex) if id in indexKeys else {})} for id in idList }

def makeTree(ids, childsIndex):
    
    if ids == []:
        return []

    return {id: makeTree(childsIndex.get(id, []), childsIndex) for id in ids }

def makeHTMLTree(ids, childsIndex, relsDataIndex):
    
    if ids == []:
        return ""

    html =  ''.join(f"""<ul><li id="osm-rel-{id}">{relsDataIndex[id]['tags']['name']}{makeHTMLTree(childsIndex.get(id, []), childsIndex, relsDataIndex)}</li></ul>""" for id in ids)

    return html