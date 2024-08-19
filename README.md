# how to set up

python3 -m venv venv

source venv/bin/activate

pip install -r requirements.txt

export AIRFLOW_HOME=$(pwd)/airflow_home

airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname FIRST_NAME \
    --lastname LAST_NAME \
    --role Admin \
    --email admin@example.com

mkdir airflow_home/dags
cp pipeline.py airflow_home/dags

# export AIRFLOW_HOME=$(pwd)/airflow_home on both terminal
airflow scheduler
airflow webserver --port 8080