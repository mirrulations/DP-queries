import psycopg
import os
import json
import logging

# Error classes
class DatabaseConnectionError(Exception):
    pass

class DataRetrievalError(Exception):
    pass



def get_db_connection():
    '''
    Establish connection to the PostgreSQL database
    '''

    try:
        conn = psycopg.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        )
        logging.info("Database connection successful.")
        return conn

    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise DatabaseConnectionError("Database connection failed")


def append_docket_fields(dockets_list, db_conn=None):
    '''
    Append additional fields from dockets table using docket ids from OpenSearch query results
    '''
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Use provided db_conn or create one for normal operation
    conn = db_conn if db_conn else get_db_connection()
    cursor = conn.cursor()


    try:
        # Extract docket IDs from dockets list
        docket_ids = [item["id"] for item in dockets_list]

        # Query to fetch docket fields
        query = """
        SELECT docket_id, docket_title, modify_date, docket_type
        FROM dockets 
        WHERE docket_id = ANY(%s)
        """

        cursor.execute(query, (docket_ids,))

        # Fetch results and format them as JSON
        results = cursor.fetchall()
        docket_titles = {row[0]: row[1] for row in results}
        modify_dates = {row[0]: row[2].isoformat() for row in results}
        docket_types = {row[0]: row[3] for row in results}
        #docket_abstracts = {row[0]: row[4] for row in results}


        # Append additional fields to the dockets list
        for item in dockets_list:
            item["title"] = docket_titles.get(item["id"], "Title Not Found")
            item["timelineDates"] = {}
            item["timelineDates"]["dateModified"] = modify_dates.get(item["id"], "Date Not Found")
            item["docketType"] = docket_types.get(item["id"], "Docket Type Not Found")
            #item["summary"] = docket_abstracts.get(item["id"], "Docket Summary Not Found")


        dockets_list = [item for item in dockets_list if item["title"] != "Title Not Found"]

        logging.info("Successfully appended additional fields.")

    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")
        raise DataRetrievalError("Failed to retrieve additional fields.")

    finally:
        cursor.close()
        if not db_conn:
            conn.close()
        logging.info("Database connection closed.")

    # Return the updated list
    return dockets_list


def append_agency_fields(dockets_list, db_conn=None):
    '''
    Append agency fields using docket ids from OpenSearch query results
    '''
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Use provided db_conn or create one for normal operation
    conn = db_conn if db_conn else get_db_connection()
    cursor = conn.cursor()

    try:
        # Extract docket IDs from dockets list
        docket_ids = [item["id"] for item in dockets_list]

        # Query to fetch agency fields
        query = """
        SELECT d.docket_id, a.agency_id, a.agency_name
        FROM dockets d 
        JOIN agencies a 
        ON d.agency_id = a.agency_id 
        WHERE d.docket_id = ANY(%s)
        """

        cursor.execute(query, (docket_ids,))

        # Fetch results and format them as JSON
        results = cursor.fetchall()
        agency_ids = {row[0]: row[1] for row in results}
        agency_names = {row[0]: row[2] for row in results}

        # Append agency fields to the dockets list
        for item in dockets_list:
            item["agencyID"] = agency_ids.get(item["id"], "Agency Not Found")
            item["agencyName"] = agency_names.get(item["id"], "Agency Name Not Found")

        logging.info("Successfully appended agency fields.")

    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")
        raise DataRetrievalError("Failed to retrieve agency fields.")

    finally:
        cursor.close()
        if not db_conn:
            conn.close()
        logging.info("Database connection closed.")

    # Return the updated list
    return dockets_list


def append_document_counts(dockets_list, db_conn=None):
    '''
    Appends total document count and comment status from documents table to each docket in the dockets list.
    Any docket not found in the query results will default to:
      - documentCount = 0
      - isOpenForComment = False
    '''
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    conn = db_conn if db_conn else get_db_connection()
    cursor = conn.cursor()

    try:
        docket_ids = [item["id"] for item in dockets_list]

        # BOOL_OR returns True if any value is True for is_open_for_comment
        query = """
        SELECT docket_id,
               COUNT(document_id) AS document_count,
               BOOL_OR(is_open_for_comment) AS is_open
        FROM documents  
        WHERE docket_id = ANY(%s)
        GROUP BY docket_id
        """

        cursor.execute(query, (docket_ids,))
        results = cursor.fetchall()

        # Lookup dict by docket_id
        document_info = {
            row[0]: {
                "total": row[1],
                "is_open": row[2]
            }
            for row in results
        }

        # Append document counts and isOpenForComment status to dockets list
        for item in dockets_list:
            info = document_info.get(item["id"], {"total": 0, "is_open": False})
            item["documentCount"] = info["total"]
            item["isOpenForComment"] = info["is_open"]

        logging.info("Successfully appended document counts and comment status to dockets.")

    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")
        raise DataRetrievalError("Failed to retrieve document counts.")

    finally:
        cursor.close()
        if not db_conn:
            conn.close()
        logging.info("Database connection closed.")

    # Return the updated list
    return dockets_list


def append_document_dates(dockets_list, db_conn=None):
    '''
    Append document date fields (first posted date, comments open date, comments close date)
    from the documents table to each docket in the dockets_list.
    If no document data is found for a docket, the fields will be set to None.
    '''
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Use provided db_conn or create one for normal operation
    conn = db_conn if db_conn else get_db_connection()
    cursor = conn.cursor()

    try:
        # Extract docket IDs from the dockets_list
        docket_ids = [item["id"] for item in dockets_list]

        # Query to fetch aggregated document date fields for each docket
        query = """
        SELECT
            docket_id,
            MIN(posted_date) AS dateCreated,
            MIN(comment_start_date) AS dateCommentsOpened,
            MAX(comment_end_date) AS dateClosed,
            MIN(effective_date) AS dateEffective
        FROM documents
        WHERE docket_id = ANY(%s)
        GROUP BY docket_id
        """

        cursor.execute(query, (docket_ids,))
        results = cursor.fetchall()

        # Create lookup dictionaries for each date field, formatting dates as ISO strings if they are not None.
        first_posted_dates = {
            row[0]: row[1].isoformat() if row[1] is not None else None
            for row in results
        }
        comments_open_dates = {
            row[0]: row[2].isoformat() if row[2] is not None else None
            for row in results
        }
        comments_close_dates = {
            row[0]: row[3].isoformat() if row[3] is not None else None
            for row in results
        }

        effective_dates = {
            row[0]: row[4].isoformat() if row[4] is not None else None
            for row in results
        }

        '''
        Append the document date fields to each docket in the list
        None will be returned if dates aren't found, conforming to API spec requiring null dates
        '''
        for item in dockets_list:
            docket_id = item["id"]
            item["timelineDates"]["dateCreated"] = first_posted_dates.get(docket_id) 
            item["timelineDates"]["dateCommentsOpened"] = comments_open_dates.get(docket_id) 
            item["timelineDates"]["dateClosed"] = comments_close_dates.get(docket_id) 
            item["timelineDates"]["dateEffective"] = effective_dates.get(docket_id) 

        logging.info("Successfully appended document dates.")

    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")
        raise DataRetrievalError("Failed to retrieve document dates.")

    finally:
        cursor.close()
        if not db_conn:
            conn.close()
        logging.info("Database connection closed.")

    # Return the updated dockets list
    return dockets_list


def append_summary(dockets_list, db_conn=None):
    """
    For each docket, append the abstract if available (and 10 or more words), otherwise append the HTM summary.
    If neither exists, return None (null).
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    conn = db_conn if db_conn else get_db_connection()
    cursor = conn.cursor()

    try:
        docket_ids = [item["id"] for item in dockets_list]

        # Query to get abstracts where abstracts are not null and have at least 10 words
        cursor.execute("""
            SELECT docket_id, abstract
            FROM abstracts
            WHERE docket_id = ANY(%s)
              AND abstract IS NOT NULL
              AND array_length(regexp_split_to_array(abstract, '\s+'), 1) > 9
        """, (docket_ids,))

        # Fetch results and format them as JSON
        abstract_results = cursor.fetchall()
        abstract_lookup = {row[0]: row[1] for row in abstract_results}

        # Find docket IDs that are missing abstracts to query HTM summaries
        missing_abstract_ids = list(set(docket_ids) - set(abstract_lookup.keys()))

        # Query to return most recent HTM summary (based on largest summary_id)
        if missing_abstract_ids:
            cursor.execute("""
                SELECT DISTINCT ON (docket_id)
                    docket_id, summary
                FROM htm_summaries
                WHERE docket_id = ANY(%s) AND summary IS NOT NULL
                ORDER BY docket_id, summary_id DESC
            """, (missing_abstract_ids,))

            summary_results = cursor.fetchall()
            summary_lookup = {row[0]: row[1] for row in summary_results}
        else:
            summary_lookup = {}

        # Merge results into dockets_list, returns None if no summary is found
        for item in dockets_list:
            docket_id = item["id"]
            if docket_id in abstract_lookup:
                item["summary"] = abstract_lookup[docket_id]
            elif docket_id in summary_lookup:
                item["summary"] = summary_lookup[docket_id]
            else:
                item["summary"] = None

        logging.info("Successfully appended summary info.")

    except Exception as e:
        logging.error(f"Error retrieving summaries: {e}")
        raise

    finally:
        cursor.close()
        if not db_conn:
            conn.close()
        logging.info("Database connection closed.")

    return dockets_list
