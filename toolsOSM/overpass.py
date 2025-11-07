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
        return {"status": "error", "status_type": "network_timeout", "data": None}
    except requests.RequestException as e:
        return {"status": "error", "status_type": str(e), "data": None}

    try:
        data = response.json()
    except ValueError:
        return {"status": "error", "status_type": "invalid_json", "data": None}

    # check overpass internal response
    if "remark" in data and "timed out" in data["remark"].lower():
        return {"status": "error", "status_type": "overpass_timeout", "data": data}
    if len(data["elements"]) == 0:
        return {"status": "error", "status_type": "missing_elements", "data": data}

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
        return {"status": "error", "status_type": "network_timeout", "data": None}
    except requests.RequestException as e:
        return {"status": "error", "status_type": str(e), "data": None}

    try:
        data = response.json()
    except ValueError:
        return {"status": "error", "status_type": "invalid_json", "data": None}

    # check overpass internal response
    if "remark" in data and "timed out" in data["remark"].lower():
        return {"status": "error", "status_type": "overpass_timeout", "data": data}
    if len(data["elements"]) == 0:
        return {"status": "error", "status_type": "missing_elements", "data": data}

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

    #* this uses the centroid of the relation
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
        return {"status": "error", "status_type": "missing_center"}
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

def _test_node_type(child_id, parent_id, node_type, query):

    logger.info(f"   * Getting {node_type}:")
    query_result = osm_query_safe_wrapper(query)
    
    # failed (network error, timeout, etc.)
    if query_result['status'] != "ok":
        return {
            'status': 'error',
            'result': None,
            'status_type': f"Error getting [{node_type}]: {query_result.get('status_type')}",
            'node': [node_type, None]
        }
    
    # query succeeded but no nodes found
    if len(query_result['data']['elements']) == 0:
        return {
            'status': 'missing',
            'result': None,
            'status_type': 'missing_node',
            'node': [node_type, None]
        }
    
    # node found - test if it's inside parent
    node_id = query_result['data']['elements'][0]['id']
    logger.info(f"   * Testing {node_type} node (id: {node_id})")
    test_result = is_node_inside_rel(node_id, parent_id, node_type)

    return test_result


def is_center_inside_parent(child_id, parent_id):
    # [OLD]
    # query = f"""
    #     [out:json][timeout:300];
    #     rel({child_id})->.r;
	# 	node(r.r:"label");
	# 	convert node ::id = id(), role='label';
	# 	out tags;
    # """

    results = {}

    # Test admin_centre node
    query = f"""
        [out:json][timeout:300];
        rel({child_id})->.r;
        node(r.r:"admin_centre");
		out ids 1;
    """
    results["admin_centre"] = _test_node_type(child_id, parent_id, "admin_centre", query)

    # Test label node
    query = f"""
        [out:json][timeout:300];
        rel({child_id})->.r;
        node(r.r:"label");
		out ids 1;
    """
    results["label"] = _test_node_type(child_id, parent_id, "label", query)

    # Test place node
    query = f"""
        [out:json][timeout:300];
        rel({child_id})->.r;
        node(r.r)[place];
		out ids 1;
    """
    results["place"] = _test_node_type(child_id, parent_id, "place", query)
    
    return results



def is_node_inside_rel(node_id, rel_id, node_type):

    query = f"""
        [out:json][timeout:300];
        rel({rel_id});
        map_to_area;
        node({node_id})->.testnode;
        node.testnode(area);
        out ids;
    """
    result = osm_query_safe_wrapper(query)

    # normalize results - always return consistent structure
    if result['status'] == 'ok' and len(result['data']['elements']) == 0:
        return {
            'status': 'ok',
            'result': False,
            'status_type': None,
            'node': [node_type, node_id]
        }
    elif result['status'] == 'ok':
        found_node_id = result['data']['elements'][0]['id']
        return {
            'status': 'ok',
            'result': (found_node_id == node_id),
            'status_type': None,
            'node': [node_type, node_id]
        }
    else:
        # error case - normalize to consistent structure
        return {
            'status': 'error',
            'result': None,
            'status_type': f"Error testing node inside [{node_type}]: {result.get('status_type')}",
            'node': [node_type, node_id]
        }

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
            return {"status": "ok", "status_type": None, "data": data}

        except requests.exceptions.Timeout:
            status_type = "network_timeout"
        except requests.exceptions.RequestException as e:
            status_type = f"http_error: {e}"
            if e.response.status_code == 400:
                break
        except Exception as e:
            status_type = str(e)

        logger.info(f"   * Attempt {attempt+1} failed: {status_type}")
        time.sleep(min(2**attempt, 15))

    return {"status": "error", "status_type": status_type, "data": None}