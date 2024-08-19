## How to Set Up

1. Create a virtual environment and activate it:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up the Airflow home directory:

    ```bash
    export AIRFLOW_HOME=$(pwd)/airflow_home
    ```

4. Initialize the Airflow database:

    ```bash
    airflow db init
    ```

5. Create an admin user:

    ```bash
    airflow users create \
        --username admin \
        --firstname FIRST_NAME \
        --lastname LAST_NAME \
        --role Admin \
        --email admin@example.com
    ```

6. Prepare the Airflow DAGs directory and copy pipeline script:

    ```bash
    mkdir airflow_home/dags
    cp pipeline.py airflow_home/dags
    ```

7. Export `AIRFLOW_HOME` on both terminals and start the Airflow scheduler and webserver:

    ```bash
    export AIRFLOW_HOME=$(pwd)/airflow_home
    ```

    ```bash
    airflow scheduler
    airflow webserver --port 8080
    ```