# Query in Dev Environment

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