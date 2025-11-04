import requests
import os
import json
import copy
import pandas as pd
import time
import logging

logger = logging.getLogger('dup_test_logger')

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
        response = requests.get(endPoint, params={"data": query})
        response.raise_for_status()
    except requests.exceptions.Timeout:
        return {"status": "error", "error_type": "network_timeout", "data": None}
    except requests.RequestException as e:
        return {"status": "error", "error_type": str(e), "data": None}

    try:
        data = response.json()
    except ValueError:
        return {"status": "error", "error_type": "invalid_json", "data": None}

    # check overpass internal response
    if "remark" in data and "timed out" in data["remark"].lower():
        return {"status": "error", "error_type": "overpass_timeout", "data": data}
    if len(data["elements"]) == 0:
        return {"status": "error", "error_type": "missing_elements", "data": data}

    return {"status": "ok", "data": data}


def getOSMAdds(relId: str, lvls: list, type: str):

    match type:
        case "recurseDown":
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
        response = requests.get(endPoint, params={"data": query})
        response.raise_for_status()
    except requests.exceptions.Timeout:
        return {"status": "error", "error_type": "network_timeout", "data": None}
    except requests.RequestException as e:
        return {"status": "error", "error_type": str(e), "data": None}

    try:
        data = response.json()
    except ValueError:
        return {"status": "error", "error_type": "invalid_json", "data": None}

    # check overpass internal response
    if "remark" in data and "timed out" in data["remark"].lower():
        return {"status": "error", "error_type": "overpass_timeout", "data": data}
    if len(data["elements"]) == 0:
        return {"status": "error", "error_type": "missing_elements", "data": data}

    return {"status": "ok", "data": data}


def makeJSTree(idList, childsIndex, relsDataIndex):
    # return 'D'
    return [
        {
            "id": id,
            "text": relsDataIndex.get(id)["tags"]["name"],
            "children": makeJSTree(childsIndex.get(id, []), childsIndex, relsDataIndex),
        }
        for id in idList
    ]


def makeTree(ids, childsIndex):

    if ids == []:
        return []

    return {id: makeTree(childsIndex.get(id, []), childsIndex) for id in ids}


def makeHTMLTree(ids, childsIndex, relsDataIndex):

    if ids == []:
        return ""

    html = "".join(
        f"""<ul><li id="osm-rel-{id}">{next((relsDataIndex[id]['tags'][key] for key in ['name:en','name'] if key in relsDataIndex[id]['tags']))}{makeHTMLTree(childsIndex.get(id, []), childsIndex, relsDataIndex)}</li></ul>"""
        for id in ids
    )

    return html

#* [OLD]
# def normalizeOSM(raw):
#     normalized = copy.deepcopy(raw)
#     normalized = {str(ele["id"]): ele for ele in raw["elements"]}
#     for id in normalized.keys():
#         normalized[id]["id"] = str(normalized[id]["id"])
#     return normalized


def is_centroid_inside_parent(childId, parentId):

    #* this uses the centroid 'center' of the relation
    #* is not necessarily inside the child
    query = f"""
        [out:json][timeout:300];

        rel({childId});
        out center;
    """
    logger.info("   * Getting center of child: ")
    centerRes = osm_query_safe_wrapper(query)
    
    if centerRes["status"] == "ok" and len(centerRes["data"]["elements"])>0:
        center = centerRes["data"]["elements"][0]["center"]
        lat, lon = center["lat"], center["lon"]
    else:
        return {"status": "error", "error_type": "missing_center", "data": centerRes['data']}
    query = f"""
        [out:json][timeout:300];

        is_in({lat}, {lon})->.areas;
        rel(pivot.areas)(id:{parentId});
        out ids;
    """
    logger.info("   * Getting parent that contains center: ")
    result = osm_query_safe_wrapper(query)

    # check result if parent contains child
    if result['status'] == 'ok' and len(result['data']['elements']) == 0:
        return {'status':'ok', 'result': False}
    elif result['status'] == 'ok':
        parent = result['data']['elements'][0]
        if str(parent['id']) == parentId:
            return {'status':'ok', 'result': True}
        else:
            return {'status':'ok', 'result': False}

    # forward other results
    return result

def is_center_inside_parent(child_id, parent_id):

    #* try admin_centre and label first
    #* if they are empty then use node[place]
    #! [old query]
    # query = f"""
    #     [out:json][timeout:300];
    #     rel({child_id})->.r;
    #     (
    #     node(r.r:"admin_centre");
    #     node(r.r:"label");
    #     )->.candidates;

    #     .candidates out ids;
    # """

    query = f"""
        [out:json][timeout:300];
        rel({child_id})->.r;
        
        node(r.r:"admin_centre");
		convert node ::id = id(), role='admin_centre';
		out tags;

		node(r.r:"label");
		convert node ::id = id(), role='label';
		out tags;
    """
    logger.info("   * Getting admin_centre and label:")
    node_center_res = osm_query_safe_wrapper(query)
    if(node_center_res['status'] == "error"): return node_center_res

    if node_center_res["status"] == "ok" and len(node_center_res["data"]["elements"]) > 0:
        #* try 'admin_centre'
        center_ac_id = [ele for ele in node_center_res["data"]["elements"] if ele['tags']['role'] == "admin_centre"][0]["id"]
        is_inside_res = is_node_inside_rel(center_ac_id, parent_id)

        if(is_inside_res['status'] == "error"): return is_inside_res
        if is_inside_res['result']: return is_inside_res
        
        #* try 'label'
        center_ac_label = [ele for ele in node_center_res["data"]["elements"] if ele['tags']['role'] == "label"][0]["id"]
        is_inside_res = is_node_inside_rel(center_ac_label, parent_id)

        if(is_inside_res['status'] == "error"): return is_inside_res
        if is_inside_res['result']: return is_inside_res

    query = f"""
        [out:json][timeout:300];
        rel({child_id})->.r;
        node(r.r)[place];
        out ids;
    """
    if node_center_res["status"] == "ok" and len(node_center_res["data"]["elements"]) == 0:
    #* try labels

        logger.info("   * missing ['admin_centre','label','place']; fallback to centroid test")
        return is_centroid_inside_parent(child_id, parent_id)
    else:
        return {"status": "error", "result": "missing_center", "data": node_center_res['data']}



def is_node_inside_rel(node_id, rel_id):
    query = f"""
        [out:json][timeout:300];
        rel({rel_id});
        map_to_area;
        node({node_id})->.testnode;
        node.testnode(area);
        out ids;
    """
    logger.info("   * Getting parent that contains center: ")
    result = osm_query_safe_wrapper(query)

    # check result if parent contains child
    if result['status'] == 'ok' and len(result['data']['elements']) == 0:
        return {'status':'ok', 'result': False}
    elif result['status'] == 'ok':
        found_node_id = result['data']['elements'][0]['id']
        if found_node_id == node_id:
            return {'status':'ok', 'result': True}
        else:
            return {'status':'ok', 'result': False}

    # forward other results
    return result

def normalizeOSM(elems):
    df = pd.json_normalize(elems)
    df = df.convert_dtypes()

    columns = [
        'type',
        'id',
        'tags.admin_level',
        'tags.parent_id',
        'tags.name',
        'tags.name:us',
        'tags.ISO3166-1',
        'tags.ISO3166-2',
        'tags.is_in:country',
        'tags.ref:nuts',
        'tags.ref:nuts:2',
        'tags.ref:nuts:3',
        "tags.addr:country",
        "tags.country_name",
        "tags.country_id"
    ]

    existing_cols = df.columns
    for col in columns:
        if col not in existing_cols:
            df[col] = pd.NA

    # sub dataframe with only columns
    df = df[columns]

    df = df.astype('string')
    return df


def osm_query_safe_wrapper(query, max_retries=5):

    endPoint = "http://overpass-api.de/api/interpreter"

    for attempt in range(max_retries):
        try:
            response = requests.get(endPoint, params={"data": query}, timeout=60)
            response.raise_for_status()

            data = response.json()

            # check overpass internal timed out response
            if "remark" in data and "timed out" in data["remark"].lower():
                raise Exception("overpass_timeout")
            
            # success return
            logger.info(f"   * ok")
            return {"status": "ok", "data": data}
        
        except requests.exceptions.Timeout:
            error_type = "network_timeout"
        except requests.exceptions.RequestException as e:
            error_type = f"http_error: {e}"
        except Exception as e:
            error_type = str(e)

        logger.info(f"   * Attempt {attempt+1} failed: {error_type}")
        time.sleep(min(2**attempt, 15))

    return {"status": "error", "result": error_type, "data": None}