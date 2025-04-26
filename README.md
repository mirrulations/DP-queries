# DP-queries
This repository contains query scripts created by Data Product for use in API

## Query in Dev Environment

To use this, set up a `.env` file with the following fields:
```
OPENSEARCH_INITIAL_ADMIN_PASSWORD=anything
OPENSEARCH_HOST=localhost
OPENSEARCH_PORT=9200
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=anything
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

Then, run `query.py` and pass the search term as a command line argument. As we include more of the query specification, those will either be other command line arguments or we can find another way to do this.

## Note about Pagination for Next Capstone Class

Currently, pagination is extremely slow. This is primarily because the refreshResults query parameter is always given as `true` from the API. Note that the pagination cache only store *matching comments* and *total comments*. To avoid needing to run another OpenSearch query, it should also store *matching attachments* and *total attachments*. We are unsure if it is better to re-query the SQL database for the supplemental data or to store that in the pagination cache as well. Finally, the cache will accumulate over time as it never gets cleared. It would probably be advisable to write a lambda function to periodically drop all old stored results.