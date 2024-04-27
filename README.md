# gpn_test

## Установка Airflow
```
mkdir /opt/airflow
cd /opt/airflow
wget https://github.com/bdgdnl/gpn_test/blob/main/docker-compose.yaml
mkdir -p ./dags ./logs ./plugins ./config ./files
wget -P ./files https://github.com/bdgdnl/gpn_test/blob/main/t_employee.csv
echo -e "AIRFLOW_UID=$(id -u)" > .env
AIRFLOW_UID=50000
docker compose up airflow-init
docker compose up -d
```
