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

__3.    Наполнить таблицу данными 10-20 записей, 2-3 подразделения, файл *.csv разделители «;»__
   
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

Фрагмент таблицы t_employee_history:

| department | position                  | employee_id | full_name                        | birth_date | address       | phone1        | phone2        | month | worked_hours | action_flag | start_date              | end_date                |
|------------|---------------------------|-------------|----------------------------------|------------|---------------|---------------|---------------|-------|--------------|-------------|-------------------------|-------------------------|
| ОПД       | Бизнес-аналитик           | 00008       | Терентьева Анна Васильевна        | 1991-03-17 | Парковая 4\5 | +799944433322 | +799933322211 | 08    | 125          | I           | 2024-04-27 14:52:39.277 | 9999-12-31 00:00:00.000 |
| ОПД       | Data Scientist            | 00009       | Кузнецов Максим Андреевич        | 1984-11-22 | Солнечная 12\6 | +799922211100 | +799911122233 | 09    | 140          | I           | 2024-04-27 14:52:39.277 | 9999-12-31 00:00:00.000 |
| ...        | ...                       | ...         | ...                              | ...        | ...           | ...           | ...           | ...   | ...          | ...         | ...                     | ...                     |

Фрагмент таблицы t_employee_current:

| department | position                     | employee_id | full_name                        | birth_date | address        | phone1        | phone2        | month | worked_hours | need_update |
|------------|------------------------------|-------------|----------------------------------|------------|---------------|---------------|---------------|-------|--------------|-------------|
| УСД        | Big Data инженер             | 00015       | Поляков Даниил Владимирович       | 1990-06-25 | Луговая 8\7  | +79998887766  | +79997766544  | 15    | 130          | false       |
| УСД        | Аналитик больших данных       | 00016       | Васильева Людмила Александровна  | 1986-09-14 | Речная 10\8  | +799966655544 | +799955554433 | 16    | 115          | false       |

Теперь внесём какие-либо изменения в какую-либо из строк, например, Поляков Даниил теперь стал старшим Big Data инженером. Изменим соответствущий аттрибут и проставим флаг need_update true.

В таблице t_employee_current осталась только одна актуальная запись, общее количество не изменилось, в таблице t_employee_history стало на одну строку больше.

| department | position                  | employee_id | full_name             | birth_date | address     | phone1       | phone2       | month | worked_hours | action_flag | start_date              | end_date              |
|------------|---------------------------|-------------|-----------------------|------------|-------------|--------------|--------------|-------|--------------|-------------|-------------------------|-----------------------|
| УСД        | Старший Big Data инженер  | 00015       | Поляков Даниил Владимирович | 1990-06-25 | Луговая 8\7 | +79998887766 | +79997766544 | 15    | 130          | U           | 2024-04-27 20:23:26.517 | 9999-12-31 00:00:00.000 |
| УСД        | Big Data инженер           | 00015       | Поляков Даниил Владимирович | 1990-06-25 | Луговая 8\7 | +79998887766 | +79997766544 | 15    | 130          | I           | 2024-04-27 14:52:39.277 | 2024-04-27 20:23:26.517 |

Допустим, Даниил Поляков зазнался и стал плохо работать. Уволим его, то есть удалим строку с информацией о нём

Изменения соответствующим образом отразятся в t_employee_history:

| department | position                  | employee_id | full_name             | birth_date | address     | phone1       | phone2       | month | worked_hours | action_flag | start_date              | end_date              |
|------------|---------------------------|-------------|-----------------------|------------|-------------|--------------|--------------|-------|--------------|-------------|-------------------------|-----------------------|
| УСД        | Big Data инженер           | 00015       | Поляков Даниил Владимирович | 1990-06-25 | Луговая 8\7 | +79998887766 | +79997766544 | 15    | 130          | I           | 2024-04-27 14:52:39.277 | 2024-04-27 20:23:26.517 |
| УСД        | Старший Big Data инженер  | 00015       | Поляков Даниил Владимирович | 1990-06-25 | Луговая 8\7 | +79998887766 | +79997766544 | 15    | 130          | D           | 2024-04-27 20:31:06.225 | 2024-04-27 20:31:06.225 |
| УСД        | Старший Big Data инженер  | 00015       | Поляков Даниил Владимирович | 1990-06-25 | Луговая 8\7 | +79998887766 | +79997766544 | 15    | 130          | U           | 2024-04-27 20:23:26.517 | 2024-04-27 20:31:06.225 |

## Кейс #2

1.	Метод возвращает список из 500 записей начинающихся с буквы «А». Предложить структуру хранения возвращаемого списка Allpages
2.	Создать таблицы для хранения данных в Greenplum DBMS
3.	При помощи Airflow разобрать возвращаемый JSON, результат поместить в созданную структуру

Таблица по запуска dag
<img width="344" alt="image" src="https://github.com/bdgdnl/gpn_test/assets/98325554/557471f4-dcd2-4f8f-9b5c-aad887ad99e0">


