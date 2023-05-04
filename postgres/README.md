# Postgres server to store data from DAG

If you want to use a postgres server running in a docker container to store date from the `data_ingest_local` DAG just follow these steps.

1. Spin up the Airflow containers.
2. Find the network name of these containers:
    ```
    $ docker network ls

    NETWORK ID     NAME             DRIVER    SCOPE
    f3aac04e474f   bridge           bridge    local
    8833a229f912   dtc-de_default   bridge    local
    133bdfd4e532   host             host      local
    5533ff6d34d8   none             null      local
    ```

    In my case the name of the network is `dtc-de-default`
3. Define the network in the .env fie:
    ```
    NETWORK_NAME=dtc-de_default
    ```
4. Spin up the postgres and pg-admin containers by running:
    `docker compose up` from within the current folder.

## NOTE:

By default the airflow web interface is accessible on `localhost:8080`.
This is why we need another port for the pg-admin web UI. `localhost:8081`

login to pg-admin by the user and password defined in the `.env`
```
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=root
```

By default postgres uses the port 5432, in our case this is already taken by the Airflow postgres db. For this second postgres db we use port 5433.

So, if you want to connect to this db from the host machine you will need to use port 5433. However, if you want to connect from the pg-admin UI you will need to connect to:

```
host: pgdatabase # name od service is defined in the `docker-compose.yaml`
port: 5432 # postgres default
user: root # as defined in the .env
pwd: root  # as defined in the .env
```
