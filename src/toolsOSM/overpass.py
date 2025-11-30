import requests
import os
import json
import copy
import pandas as pd
import time
import logging
import toolsGeneral.main as tgm
import toolsGeneral.logger as tgl
from pathlib import Path
import re

logger = logging.getLogger('logger')
raw_scrape_logger = logging.getLogger('raw_scrape_logger')

def getOSMIDAddsStruct(relId: str, lvls: list):

    query = f"""
        [timeout:600][out:json];

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
    query_res = osm_query_safe_wrapper(query)

    if query_res['status'] == 'ok' and query_res['data']['elements'] == 0:
        return {"status": "error", "status_type": "missing_elements", "data": query_res['data']}

    return query_res

def fetch_level_in_chunks(from_lvl, to_lvl, parent_ids, save_dir:Path, chunk_start_index, state, chunk_size=20):
    failed = set()
    processed = set()
    discovered = set()

    # ids_to_process = [pid for pid in parent_ids if pid not in processed]
    chunks = [parent_ids[i:i+chunk_size] for i in range(0, len(parent_ids), chunk_size)]
    os.makedirs(save_dir, exist_ok=True)

    next_chunk_index = chunk_start_index
    raw_scrape_logger.info(f" > processing {from_lvl} to {to_lvl}:")
    raw_scrape_logger.info(f"  * ids in level ({from_lvl}): {len(state[from_lvl]['discovered'])}:")
    raw_scrape_logger.info(f"  * current ids to process: {len(parent_ids)}, number of chunks = {len(chunks)}")
    
    
    for chunk_idx, chunk in enumerate(chunks, start=chunk_start_index):
        raw_scrape_logger.info(f"  > chunk_{chunk_idx}")
        raw_scrape_logger.info(f"   * making query ...")
        res = get_add_lvls_from_id(chunk, to_lvl)
        raw_scrape_logger.info(f"   * finished query ...")
        if res.get("status") != "ok":
            failed.update(chunk)
            continue
        
        processed.update(chunk)
        
        elements = res["data"].get("elements", [])
        if elements:
            next_chunk_index = chunk_idx + 1
            discovered.update(str(elem["id"]) for elem in elements if "id" in elem)

        # save state from chunk
        state[from_lvl]['processed'].update(processed)
        state[from_lvl]['failed'].update(failed)

        state[to_lvl]['next_chunk_index'] = next_chunk_index
        state[to_lvl]['discovered'].update(discovered)

        # save data
        raw_scrape_logger.info(f"   * saving...")
        tgm.dump(os.path.join(save_dir / f'lvl_{to_lvl}_chunk_{chunk_idx}_rawOSMRes.json'), res['data'])
        tgm.dump(os.path.join(save_dir / 'state.pkl'), state)

        raw_scrape_logger.info(f"   * processed: {len(processed)}, failed: {len(failed)}, next level ({to_lvl}) discovered: {len(discovered)}")
        raw_scrape_logger.info(f"  > finished chunk_{chunk_idx}")
        
    raw_scrape_logger.info(f" > finished lvl {from_lvl}-> processed:{len(processed)}, failed: {len(failed)}, next level discovered: {len(discovered)}")

    return processed, next_chunk_index, failed, discovered

def getOSMIDAddsStruct_chunks(tuple, save_dir:Path):

    # initialize variables
    country, id, addLvls = tuple
    country_save_dir = save_dir / country
    
    retries_max = 5
    retries_count = 0
    lvls = ['2', *addLvls]
    state_path = country_save_dir / 'state.pkl'
    if os.path.exists(state_path):
        state = tgm.load(state_path)
    else:
        state = {}
        for lvl in lvls:
            state[lvl] = {"processed": set(), "failed": set(), "discovered": set(), "next_chunk_index": 0}


    # Get country data and save file
    state['2']['discovered'] = {id}
    country_osm_data = getOSMIDAddsStruct(id, [-1,-1,-1])
    tgm.dump(country_save_dir / f'lvl_2_chunk_0_rawOSMRes.json', country_osm_data['data'])

    # get levels with retry
    raw_scrape_logger.info(f" > processing {country}:")
    def fetch_level_with_retry(from_lvl, to_lvl, state):

        pending = [id for id in state[from_lvl]['discovered'] if id not in state[from_lvl]['processed']]

        while pending and retries_count <= retries_max:
            processed, next_chunk_index, failed, discovered = fetch_level_in_chunks(
                from_lvl=from_lvl, 
                to_lvl=to_lvl,
                parent_ids=pending,
                save_dir=country_save_dir,
                chunk_start_index=state[to_lvl]['next_chunk_index'],
                state=state,
                chunk_size=50
            )

            pending = failed
            retries_count += 1
            if failed:
                raw_scrape_logger.info(f"Retrying {len(failed)} failed IDs...")

    fetch_level_with_retry('2', '4', state)
    fetch_level_with_retry('4', '6', state)
    fetch_level_with_retry('6', '8', state)

    if retries_count > retries_max:
        res = {'staus':'error', 'status_type': 'max_number_retries', 'data':state}
    else:
        res = {'staus':'ok', 'status_type': None, 'data':state}

    return res

def get_add_lvls_from_id(ids:list, lvl:str):

    query = f"""
        [timeout:900][out:json];

        rel(id:{','.join(ids)});
        foreach{{
            ._->.elem;
            map_to_area;
            rel[boundary=administrative][admin_level={lvl}](area);
            convert rel ::id = id(), ::=::, parent_id=elem.set(id());
            out tags;
        }};
    """
    query_res = osm_query_safe_wrapper(query)
    
    if query_res['status'] == 'ok' and query_res['data']['elements'] == 0:
        return {"status": "error", "status_type": "missing_elements", "data": query_res['data']}

    return query_res

def getOSMAdds(relId: str, lvls: list, type: str):

    match type:
        case "recurseDown":
            return getOSMAddsTRecursedown(relId, lvls)


def getOSMAddsTRecursedown(relId: str, lvls: list):

    endPoint = "http://overpass-api.de/api/interpreter"

    query = f"""
        [timeout:900][out:json];

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

#*  [OLD]
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

    logger.info(f"   > Getting {node_type}:")
    query_result = osm_query_safe_wrapper(query)

    # failed (network error, timeout, etc.)
    if query_result['status'] != "ok":
        logger.info(f"    * Error getting ({node_type}): {query_result.get('status_type')}")
        return {
            'status': 'error',
            'result': None,
            'status_type': f"Error getting ({node_type}): {query_result.get('status_type')}",
            'node': [node_type, None]
        }

    # query succeeded but no nodes found
    if len(query_result['data']['elements']) == 0:
        logger.info(f"    * Missing node ({node_type})")
        return {
            'status': 'missing',
            'result': None,
            'status_type': 'missing_node',
            'node': [node_type, None]
        }
    
    # node found - test if it's inside parent
    logger.info(f"    * Found node ({node_type})")
    if node_type == 'centroid':
        center = query_result["data"]["elements"][0]["center"]
        lat, lon = center["lat"], center["lon"]
        logger.info(f"   > Testing {node_type} (lat: {lat}, lon: {lon})")
        test_result = is_node_inside_rel([lat, lon], parent_id, node_type)
    else:
        node_id = query_result['data']['elements'][0]['id']
        logger.info(f"   > Testing {node_type} node (id: {node_id})")
        test_result = is_node_inside_rel(node_id, parent_id, node_type)

    return test_result


def is_child_inside_parent(child_id, parent_id):
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

    # Test node part of geometry
    query = f"""
        [out:json][timeout:300];
        rel({child_id})->.r;
        way(r.r)->.w;
        node(w.w);
		out ids 1;
    """
    results["geom_node"] = _test_node_type(child_id, parent_id, "geom_node", query)

    # Test centroid
    query = f"""
        [out:json][timeout:300];

        rel({child_id});
        out center;
    """
    results["centroid"] = _test_node_type(child_id, parent_id, "centroid", query)
    
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

    if node_type == 'centroid':
        query = f"""
            [out:json][timeout:300];

            is_in({node_id[0]}, {node_id[1]})->.areas;
            rel(pivot.areas)(id:{rel_id});
            out ids;
        """
    result = osm_query_safe_wrapper(query)

    # normalize results - always return consistent structure
    if result['status'] == 'ok' and len(result['data']['elements']) == 0:
        logger.info(f"    * Finished testing ({node_type}): False")
        return {
            'status': 'ok',
            'result': False,
            'status_type': None,
            'node': [node_type, node_id]
        }
    elif result['status'] == 'ok':
        found_element = result['data']['elements'][0]
        found_id = found_element['id']
        
        # For centroid, check if found relation matches parent_id
        # For nodes, check if found node matches the node_id
        if node_type == 'centroid':
            result_value = (str(found_id) == str(rel_id))
        else:
            result_value = (found_id == node_id)
        
        logger.info(f"    * Finished testing ({node_type}): {result_value}")
        return {
            'status': 'ok',
            'result': result_value,
            'status_type': None,
            'node': [node_type, node_id]
        }
    else:
        # error case - normalize to consistent structure
        logger.info(f"    * Error testing ({node_type}): {result.get('status_type')}")
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
        'tags.name:en',
        'tags.alt_name:en',
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
            response = requests.get(endPoint, params={"data": query}, timeout=600)
            response.raise_for_status()

            data = response.json()

            # check overpass internal timed out response
            if "remark" in data and "timed out" in data["remark"].lower():
                raise Exception("overpass_timeout")
            
            # success return
            return {"status": "ok", "status_type": None, "data": data}

        except requests.exceptions.Timeout:
            status_type = "requests_timeout"
        except requests.exceptions.RequestException as e:
            status_type = f"http_error: {e}"
            if getattr(e.response, "status_code", None) == 400:
                break
        except Exception as e:
            status_type = str(e)

        logger.info(f"   * Attempt {attempt+1} failed: {status_type}")
        time.sleep(min(2**attempt, 15))

    return {"status": "error", "status_type": status_type, "data": None}


def osm_basic_test(df_input, save_logger_path = None):

    if save_logger_path:
        tgl.add_file_handler(logger, save_logger_path)

    #* sort the dataframe
    df = df_input.copy()
    df = df.sort_values('tags.admin_level', key=lambda col: col.astype(int)) 

    cntrRow = df["tags.ISO3166-1"].notna()
    cntrISO = df[cntrRow].iloc[0]["tags.ISO3166-1"]
    cntrName = df[cntrRow].iloc[0]["tags.country_name"]
    cntr_id = df[cntrRow].iloc[0]["id"]

    #* some elements have missing name
    miss = df[df["tags.name"].isna()]['id'].to_list()
    logger.info(f" * missing names: {len(miss)}")

    #* relations from other countries
    #* only discard the ones we are sure are not from the country
    #* pass the test otherwise

    checkTags = [
        "tags.is_in:country",
        "tags.ISO3166-2",
        "tags.ref:nuts",
        "tags.ref:nuts:2",
        "tags.ref:nuts:3",
        "tags.addr:country"
    ]
    leak = []
    in_country = []
    isInCountry = {}
    NA_result = []

    for idx, row in df.iterrows():
        true_count = 0
        false_count = 0
        osmID = str(row.get("id"))
        if str(row.get("tags.admin_level")) == '2':
            isInCountry[osmID] = True
            in_country.append((osmID, pd.NA, cntr_id))
            continue
        parentID = row.get("tags.parent_id")

        for tag in checkTags:
            # we'll check all tags to have some confidence meassure of the test
            val = row.get(tag)
            if pd.isna(val):
                continue

            # Handle is_in:country tag
            if tag == "tags.is_in:country":
                if val.strip().lower() == cntrName.strip().lower():
                    true_count += 1
                else:
                    false_count += 1
                continue

            # Handle ISO-style tags
            checkISO_res = checkISO(val, cntrISO)
            if checkISO_res is True:
                true_count += 1
            elif checkISO_res is False:
                false_count += 1
            # else NA, ignore


        # meassure confidence of results

        if true_count >= 2:
            in_country.append((osmID, parentID, cntr_id))
            isInCountry[osmID] = True
        elif false_count > 0:
            leak.append((osmID, parentID, cntr_id))
            isInCountry[osmID] = False
        elif true_count <= 1:
            # single weak signal, fallback to parent
            # except for first level (4)
            if parentID and isInCountry.get(parentID) is True and row.get('tags.admin_level') != '4':
                in_country.append((osmID, parentID, cntr_id))
                isInCountry[osmID] = True
            else:
                NA_result.append((osmID, parentID, cntr_id))
                isInCountry[osmID] = pd.NA

    logger.info(f" * relations from other countries: {len(leak)}")

    return {    
        "missing.name": miss,
        "leak": leak,
        "in_country": in_country,
        'NA_result': NA_result,
    }

def checkISO(code, cntrCode):
    if code == "": return pd.NA
    iso1 = re.search(r"^([A-Z]{2})", code)

    if not iso1:
        return pd.NA
    else:
        iso1 = iso1.group(0)

    return iso1 == cntrCode

def osm_test_center(rows, save_temp=False, save_path=''):

    total = len(rows)
    if save_temp:
        test_res = tgm.load(save_path) if os.path.exists(save_path) else {}

    for i, (idx, row) in enumerate(rows.iterrows(), start=1):
        logger.info(f'* country: {Path(save_path).parent.name}')
        tuple_id = (row["id"], row["tags.parent_id"], row["tags.country_id"])
        logger.info(f" ^ [{i}/{total}]: testing {tuple_id}:")

        res = is_child_inside_parent(row["id"], row["tags.parent_id"])
        test_res[tuple_id] = res

        resume  = {k:v['status'] for k,v in res.items()}
        status_list = [v['status'] for k,v in res.items()]

        if save_temp and 'error' not in status_list:
            logger.info(f"  * saving ...")
            tgm.dump(save_path, test_res)

        logger.info(f" $ finished: status: {resume}")
        
        time.sleep(3)
    return test_res