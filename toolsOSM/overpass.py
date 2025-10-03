import requests
import os
import json
import copy

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
    try:
      response = requests.get(endPoint, params={'data': query})
      response.raise_for_status()
    except requests.exceptions.Timeout:
      return {'status':'error', 'error_type':'network_timeout','data':None}
    except requests.RequestException as e:
      return {'status':'error', 'error_type':str(e),'data':None}
    
    try:
      data = response.json()
    except ValueError:
      return {'status':'error','error_type':'invalid_json','data':None}
    
    # check overpass internal response
    if "remark" in data and "timed out" in data["remark"].lower():
      return {"status": "error", "error_type": "overpass_timeout", "data": data}
    if len(data['elements']) == 0:
      return {"status": "error", "error_type": "missing_elements", "data": data}
    
    return {"status": "ok", "data": data}


def getOSMAdds(relId: str, lvls: list, type: str):

    match type:
        case 'recurseDown':
            return getOSMAddsTRecursedown(relId, lvls)


def getOSMAddsTRecursedown(relId: str, lvls: list):

    endPoint = "http://overpass-api.de/api/interpreter"

    query = f"""
        [timeout:3600][out:json];

        rel({relId});
        out tags;

        >> -> .firstRec;
        rel.firstRec[boundary=administrative][admin_level={lvls[0]}] -> .first;
        .first out tags;

        foreach.first -> .elem(

            .elem >> -> .secondRec;
            rel.secondRec[boundary=administrative][admin_level={lvls[1]}]->.secondCurrent;
            
            (.secondCurrent;.secondAll;)->.secondAll;

            .secondCurrent;
            convert rel ::id = id(), ::=::, parent_id=elem.set(id());
            out tags;
        );

        foreach.secondAll -> .elem(

            .elem >> -> .thirdRec;
            rel.thirdRec[boundary=administrative][admin_level={lvls[2]}]->.thirdCurrent;

            .thirdCurrent;
            convert rel ::id = id(), ::=::, parent_id=elem.set(id());
            out tags;
        );

    """

    try:
      response = requests.get(endPoint, params={'data': query})
      response.raise_for_status()
    except requests.exceptions.Timeout:
      return {'status':'error', 'error_type':'network_timeout','data':None}
    except requests.RequestException as e:
      return {'status':'error', 'error_type':str(e),'data':None}
    
    try:
      data = response.json()
    except ValueError:
      return {'status':'error','error_type':'invalid_json','data':None}
    
    # check overpass internal response
    if "remark" in data and "timed out" in data["remark"].lower():
      return {"status": "error", "error_type": "overpass_timeout", "data": data}
    if len(data['elements']) == 0:
      return {"status": "error", "error_type": "missing_elements", "data": data}
    
    return {"status": "ok", "data": data}


def makeJSTree(idList, childsIndex, relsDataIndex):
    # return 'D'
    return [
        {
            "id": id,
            "text": relsDataIndex.get(id)['tags']['name'],
            "children": makeJSTree(childsIndex.get(id,[]), childsIndex, relsDataIndex)
        } for id in idList
    ]

def makeTree(ids, childsIndex):
    
    if ids == []:
        return []

    return {id: makeTree(childsIndex.get(id, []), childsIndex) for id in ids }

def makeHTMLTree(ids, childsIndex, relsDataIndex):
    
    if ids == []:
        return ""

    html =  ''.join(f"""<ul><li id="osm-rel-{id}">{next((relsDataIndex[id]['tags'][key] for key in ['name:en','name'] if key in relsDataIndex[id]['tags']))}{makeHTMLTree(childsIndex.get(id, []), childsIndex, relsDataIndex)}</li></ul>""" for id in ids)

    return html

def normalizeOSM(raw):
    normalized = copy.deepcopy(raw)
    normalized = { str(ele['id']) : ele for ele in raw['elements']}
    for id in normalized.keys():
        normalized[id]['id'] = str(normalized[id]['id'])
    return normalized

    