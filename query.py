import json
from math import exp
from dateutil import parser as date_parser
from datetime import datetime, timezone
from queries.utils.query_opensearch import query_OpenSearch
from queries.utils.query_sql import append_docket_fields, append_agency_fields, append_document_dates, append_summary
from queries.utils.sql import connect


conn = connect()

def filter_dockets(dockets, filter_params=None):
    if filter_params is None:
        return dockets
    
    if isinstance(filter_params, str):
        try:
            filter_params = json.loads(filter_params)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON input")

    agencies = filter_params.get("agencies", [])
    date_range = filter_params.get("dateRange", {})
    docket_type = filter_params.get("docketType", "")
    
    start_date = date_parser.isoparse(date_range.get("start", "1970-01-01T00:00:00Z"))
    end_date = date_parser.isoparse(date_range.get("end", datetime.now().isoformat() + "Z"))
    
    filtered = []
    for docket in dockets:
        if agencies and docket.get("agencyID", "") not in agencies:
            continue
        
        if docket_type and docket.get("docketType", "") != docket_type:
            continue

        try:
            mod_date = date_parser.isoparse(docket.get("timelineDates", {}).get("dateModified", "1970-01-01T00:00:00Z"))
        except Exception:
            mod_date = datetime.datetime(1970, 1, 1)
        if mod_date < start_date or mod_date > end_date:
            continue
        
        filtered.append(docket)
    
    return filtered

# Sort the combined results based on the given sort_type
def sort_aoss_results(results, sort_type, desc=True):
    """
    Sort a list of JSON objects based on the given sort_type.
    
    Parameters:
        results (str or list): JSON string or list of dictionaries to be sorted.
        sort_type (str): Sorting criteria ('dateModified', 'alphaByTitle', 'relevance').
        desc (bool): Sort order, descending if True (default).
    
    Returns:
        str: JSON string of sorted results.
    """

    # If results is a JSON string, try to parse it
    if isinstance(results, str):
        try:
            results = json.loads(results)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON input")

    # Ensure results is a list
    if not isinstance(results, list):
        raise TypeError(f"Expected a list, but got {type(results)}")

    # Validate sort_type
    valid_sort_types = {'dateModified', 'alphaByTitle', 'relevance'}

    if sort_type not in valid_sort_types:
        print("Invalid sort type. Defaulting to 'dateModified'")
        sort_type = 'dateModified'


    # Sort based on the sort_type
    if sort_type == 'dateModified':

        results.sort(
            key=lambda x: datetime.fromisoformat(
                x.get('dateModified', '1970-01-01T00:00:00Z') 
            ), reverse=desc)
        
    elif sort_type == 'alphaByTitle':
        results.sort(key=lambda x: x.get('title', ''), reverse=not desc)

    elif sort_type == 'relevance':
        results.sort(key=lambda x: x.get('matchQuality', 0), reverse=desc)

    for i, docket in enumerate(results):
        docket["searchRank"] = i

    # Return sorted results as a JSON string
    return results

def drop_previous_results(searchTerm, sessionID, sortParams, filterParams):
    

    try:
        with conn.cursor() as cursor:
            delete_query = """
            DELETE FROM stored_results
            WHERE search_term = %s AND session_id = %s AND sort_asc = %s AND sort_type = %s
            AND filter_agencies = %s AND filter_date_start = %s AND filter_date_end = %s AND filter_rulemaking = %s
            """
            cursor.execute(delete_query, (searchTerm, sessionID, sortParams["desc"], sortParams["sortType"],
                                          ",".join(sorted(filterParams["agencies"])) if filterParams["agencies"] else '', filterParams["dateRange"]["start"],
                                          filterParams["dateRange"]["end"], filterParams["docketType"]))
    except Exception as e:
        print(f"Error deleting previous results for search term {searchTerm}")
        print(e)

    conn.commit()

def storeDockets(dockets, searchTerm, sessionID, sortParams, filterParams, totalResults):


    for i in range(min(totalResults, len(dockets))):
        values = (
            searchTerm,
            sessionID,
            sortParams["desc"],
            sortParams["sortType"],
            ",".join(sorted(filterParams["agencies"])) if "agencies" in filterParams else '',
            filterParams["dateRange"]["start"],
            filterParams["dateRange"]["end"],
            filterParams["docketType"],
            i,
            dockets[i]["id"],
            dockets[i]["comments"]["total"],
            dockets[i]["comments"]["match"],
            dockets[i]["matchQuality"]
        )

        # Insert into the database
        try:
            with conn.cursor() as cursor:
                insert_query = """
                INSERT INTO stored_results (
                    search_term, session_id, sort_asc, sort_type, filter_agencies, filter_date_start,
                    filter_date_end, filter_rulemaking, search_rank, docket_id, total_comments, matching_comments,
                    relevance_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting docket {dockets[i]['id']}")
            print(e)

    conn.commit()

def getSavedResults(searchTerm, sessionID, sortParams, filterParams):
    

    try:
        with conn.cursor() as cursor:
            select_query = """
            SELECT search_rank, docket_id, total_comments, matching_comments, relevance_score FROM stored_results
            WHERE search_term = %s AND session_id = %s AND sort_asc = %s AND sort_type = %s AND filter_agencies = %s
            AND filter_date_start = %s AND filter_date_end = %s AND filter_rulemaking = %s
            """
            cursor.execute(select_query, (searchTerm, sessionID, sortParams["desc"], sortParams["sortType"],
                                          ",".join(sorted(filterParams["agencies"])) if filterParams["agencies"] else '', filterParams["dateRange"]["start"],
                                          filterParams["dateRange"]["end"], filterParams["docketType"]))
            dockets = cursor.fetchall()
    except Exception as e:
        print(f"Error retrieving dockets for search term {searchTerm}")
        print(e)

    return dockets

def calc_relevance_score(docket):
    try:
        total_comments = docket.get("comments", {}).get("total", 0)
        matching_comments = docket.get("comments", {}).get("match", 0)
        ratio = matching_comments / total_comments if total_comments > 0 else 0
        modify_date = date_parser.isoparse(docket.get("timelineDates", {}).get("dateModified", "1970-01-01T00:00:00Z"))
        current_time_timezone_aware = datetime.now(timezone.utc).astimezone()
        age_days = (current_time_timezone_aware - modify_date).days
        decay = exp(-age_days / 365)
        return total_comments * (ratio ** 2) * decay
    except Exception as e:
        print(f"Error calculating relevance score for docket {docket.get('id', 'unknown')}: {e}")
        return 0

def search(search_params):

    searchTerm = search_params["searchTerm"]
    pageNumber = search_params["pageNumber"]
    refreshResults = search_params["refreshResults"]
    sessionID = search_params["sessionID"]
    sortParams = search_params["sortParams"]
    filterParams = search_params["filterParams"]

    perPage = 10
    pages = 10
    totalResults = perPage * pages

    if refreshResults:
        drop_previous_results(searchTerm, sessionID, sortParams, filterParams)

        comment_results = query_OpenSearch(searchTerm, 'comments', 'commentText')
        attachment_results = query_OpenSearch(searchTerm, 'comments_extracted_text', 'extractedText')

        os_results = []

        for docket in comment_results:
            matching_comments = comment_results.get(docket, {}).get("match", 0)
            total_comments = comment_results.get(docket, {}).get("total", 0)
            matching_attachments = attachment_results.get(docket, {}).get("match", 0)
            total_attachments = attachment_results.get(docket, {}).get("total", 0)
            if matching_comments == 0 and matching_attachments == 0:
                continue
            os_results.append(
                {
                    "id": docket,
                    "comments": {
                        "match": matching_comments,
                        "total": total_comments,
                    },
                    "attachments": {
                        "match": matching_attachments,
                        "total": total_attachments,
                    },
                }
            )

        results = append_docket_fields(os_results, conn)
        results = append_agency_fields(results, conn)
        results = append_document_dates(results, conn)
        results = append_summary(results, conn)
        results = filter_dockets(results, filterParams)

        for docket in results:
            docket["matchQuality"] = calc_relevance_score(docket)


        # print(results)

        # filtered_results = filter_dockets(results, json.loads(search_params.get('filterParams')))

        # print(filtered_results)
        # sorted_results = sort_aoss_results(results, json.loads(search_params.get('sortParams')).get('sortType'))

        # print(sorted_results)

        # sort by num comments,
        # sort by date
        sorted_results = sorted(
            results, key=lambda x: x.get("comments").get("match"), reverse=True
        )
        # sorted_results = sorted(
        #     sorted_results1,
        #     key=lambda x: date_parser.isoparse(x.get("timelineDates").get("dateModified")).year,
        #     reverse=True,
        # )

        # print(sorted_results)

        if isinstance(sortParams, str):
            sortParams = json.loads(sortParams)
        if isinstance(filterParams, str):
            filterParams = json.loads(filterParams)

        storeDockets(sorted_results, searchTerm, sessionID, sortParams, filterParams, totalResults)

        count_dockets = len(sorted_results)

        count_pages = count_dockets // perPage
        if count_dockets % perPage:
            count_pages += 1

        count_pages = min(count_pages, pages)

        ret = {
            "currentPage": pageNumber,
            "totalPages": count_pages,
            "dockets": sorted_results[
                int(perPage) * int(pageNumber) : int(perPage) * (int(pageNumber) + 1)
            ],
        }

        return ret

    else:
        dockets_raw = getSavedResults(searchTerm, sessionID, sortParams, filterParams)
        dockets = []
        for d in dockets_raw:
            dockets.append(
                {
                    "searchRank": d[0],
                    "id": d[1],
                    "comments": {"match": d[3], "total": d[2]},
                    "matchQuality": d[4],
                }
            )
        dockets = sorted(dockets, key=lambda x: x["searchRank"])
        dockets = dockets[perPage * pageNumber : perPage * (pageNumber + 1)]

        count_dockets = len(dockets)
        
        count_pages = count_dockets // perPage
        if count_dockets % perPage:
            count_pages += 1

        count_pages = min(count_pages, pages)

        dockets = append_docket_fields(dockets, conn)
        dockets = append_agency_fields(dockets, conn)
        dockets = append_document_dates(dockets, conn)
        dockets = append_summary(dockets, conn)

        ret = {"currentPage": pageNumber, "totalPages": count_pages, "dockets": dockets}

        return json.dumps(ret)


if __name__ == "__main__":
    query_params = {
        "searchTerm": "National",
        "pageNumber": 0,
        "refreshResults": True,
        "sessionID": "session1",
        "sortParams": {
            "sortType": "dateModified",
            "desc": True,
        },
        "filterParams": {
            "agencies": [],
            "dateRange": {
                "start": "1970-01-01T00:00:00Z",
                "end": "2025-03-21T00:00:00Z",
            },
            "docketType": "",
        },
    }

    searchTerm = query_params["searchTerm"]
    print(f"searchTerm: {searchTerm}")

    result = search(json.dumps(query_params))

    print(json.dumps(result, indent=4))