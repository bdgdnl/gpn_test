# gpn_test

## Установка Airflow
```shell
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

## Кейс #1
*1.	Таблица t_employee ежемесячно очищается и в неё загружается реестр из системы источника. Предложить структуру хранения предоставляемых данных, с использованием SCD2.*
*2.	Создать таблицы для хранения данных в Greenplum DBMS*
*3.	Наполнить таблицу данными 10-20 записей, 2-3 подразделения, файл *.csv разделители «;»*
*4.	При помощи Airflow загрузить файл с таблицей t_employee в созданную структуру.*
