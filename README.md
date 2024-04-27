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
**1.	Таблица t_employee ежемесячно очищается и в неё загружается реестр из системы источника. Предложить структуру хранения предоставляемых данных, с использованием SCD2.**
```sql
-- Структура данных для таблицы, в которой будут храниться актуальные значения
CREATE TABLE t_employee_current (
    department VARCHAR(50),
    position VARCHAR(50),
    employee_id VARCHAR(10),
    full_name VARCHAR(100),
    birth_date DATE,
    address VARCHAR(100),
    phone1 VARCHAR(15),
    phone2 VARCHAR(15),
    month VARCHAR(2),
    worked_hours INT,
    need_update BOOLEAN, -- ввёл новое поле, чтобы можно было отслеживать строки, в которые внесли изменения
    primary key (department, employee_id)
) DISTRIBUTED BY (department);

-- Структура данных для таблицы, в которой будет храниться история изменений
CREATE TABLE t_employee_history (
    department VARCHAR(50),
    position VARCHAR(50),
    employee_id VARCHAR(10),
    full_name VARCHAR(100),
    birth_date DATE,
    address VARCHAR(100),
    phone1 VARCHAR(15),
    phone2 VARCHAR(15),
    month VARCHAR(2),
    worked_hours INT,
    action_flag VARCHAR(1), -- тип операции над записью - удаление, изменение, добавление (D, U, I)
    start_date TIMESTAMP, -- время, когда строка стала актуальной
    end_date TIMESTAMP, -- время, когда строка перестала быть актуальной
    primary key (department, employee_id, start_date)
) DISTRIBUTED BY (department);
```
**2.	Создать таблицы для хранения данных в Greenplum DBMS**

**3.	Наполнить таблицу данными 10-20 записей, 2-3 подразделения, файл *.csv разделители «;»**
   
   Фрагмент файла:
```
  	Подразделение;Должность;Табельный номер;ФИО;Дата рождения;Адрес;Телефон 1;Телефон 2;Дата (месяц);Отработанное время;need_update
    УПД;Начальник Инженеров данных;00001;Иванов Иван Иванович;01.01.1990;Гороховая ул.16\71;+79991234567;+79992345678;01;120;0
    УПД;Инженер данных;00002;Петров Петр Петрович;05.05.1985;Садовая ул.8\23;+79998887766;+79997766544;02;110;0
    УПД;Аналитик данных;00003;Сидорова Ольга Александровна;10.12.1987;Набережная реки 5\46;+799955554433;+799944433322;03;105;0
    УПД;Программист;00004;Смирнов Игорь Владимирович;20.08.1992;Лесная ул.3\12;+799933322211;+799922211100;04;130;0
    УПД;Архитектор данных;00005;Козлов Алексей Дмитриевич;15.04.1980;Полевая 9\2;+799911122233;+799900033344;05;150;0
```
**5.	При помощи Airflow загрузить файл с таблицей t_employee в созданную структуру.**
Таблицы t_employee_current и t_employee_history пусты, в файле t_employee.csv есть записи.
Запустим наш dag

