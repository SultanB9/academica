# ПОДГОТОВКА БАЗЫ ДАННЫХ ДЛЯ АНАЛИЗА
# создание базы данных клиентов
CREATE DATABASE IF NOT EXISTS customers;

# создание таблицы о клиентах
CREATE TABLE IF NOT EXISTS customer_info (
	id_client 				INT,
    total_amount 			INT,
    gender 					VARCHAR(10),
    age 					VARCHAR(10),
    count_city 				INT,
    response_communication	INT,
    communication_3month	INT,
    tenure					INT
);

# создание таблицы о транзакциях
CREATE TABLE IF NOT EXISTS transactions_info (
	date_new			DATE,
    id_check 			INT,
    id_client			INT,
    count_products		DECIMAL(10,3),
    sum_payment			DECIMAL(10,2)
);

# загрузка данных в таблицы с соответствующих scv файлов
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\customer_info.csv"
INTO TABLE customer_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions_info.csv"
INTO TABLE transactions_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# настройка таблицы customer_info
UPDATE customer_info 
SET gender = NULL WHERE gender = '';
UPDATE customer_info 
SET age = NULL WHERE age = '';
ALTER TABLE customer_info MODIFY age INT NULL;


# ЗАДАНИЕ 1
# список клиентов с непрерывной историей за год
SELECT id_client, COUNT(DISTINCT date_new) as count 
	FROM transactions_info
GROUP BY id_client
HAVING count = (
				SELECT COUNT(DISTINCT date_new) as count
                FROM transactions_info
				ORDER BY count
			)
ORDER BY id_client;

# средний чек за весь период
SELECT ROUND(AVG(total_sum), 2) as AOV FROM (
		SELECT id_check, SUM(sum_payment) as total_sum
		FROM customers.transactions_info
		GROUP BY id_check
    ) order_;

# средняя сумма покупок за месяц
SELECT ROUND(AVG(total_sum), 2) as AMS
 FROM (        
		SELECT date_new,
				SUM(sum_payment) as total_sum 
        FROM transactions_info
		GROUP BY date_new
    ) monthly_sums;

# количество всех операции по клиенту 
SELECT id_client, COUNT(sum_payment) as count_operation FROM transactions_info
GROUP BY id_client
ORDER BY count_operation DESC;

# ЗАДАНИЕ 2
# средняя сумма чека в месяц в разрезе месяцев
SELECT month, ROUND(AVG(total_sum), 2) as AOV FROM (
		SELECT DATE_FORMAT(date_new, '%Y-%m') as month, id_check, SUM(sum_payment) as total_sum
		FROM customers.transactions_info
		GROUP BY month, id_check
		ORDER BY month, id_check
    ) order_
GROUP BY month;

# среднее количество операций в месяц
WITH monthly AS (
	SELECT date_new, COUNT(DISTINCT id_check) as operations FROM transactions_info
	GROUP BY date_new
	ORDER BY date_new
) 

SELECT ROUND(AVG(operations), 2) as avg_count_operations
FROM monthly;

# среднее количество клиентов, которые совершали операции
WITH clients AS (
	SELECT date_new, COUNT(id_client) as total_clients FROM transactions_info
	GROUP BY date_new
	ORDER BY date_new
)

SELECT ROUND(AVG(total_clients), 2) as avg_clients FROM clients;

# доля операций за месяц от общего количества операций за год 
SELECT DATE_FORMAT(date_new, '%Y-%m') as month, ROUND(COUNT(DISTINCT id_check) / (
												SELECT COUNT(DISTINCT id_check) as total_operations 
												FROM transactions_info
                                            ) * 100, 2) as share_total_count
FROM transactions_info
GROUP BY month;

# доля в месяц от общей суммы операций
SELECT DATE_FORMAT(date_new, '%Y-%m') as month, 
		ROUND(SUM(sum_payment) / (SELECT SUM(sum_payment) as total_sum FROM transactions_info) * 100, 2) as share_total_count
FROM transactions_info
GROUP BY month;

# соотношение долей затрат по полам
SELECT gender, SUM(sum_payment) as total_spending,
        ROUND(SUM(sum_payment) / (SELECT SUM(sum_payment) FROM transactions_info) * 100, 2) as percent_spending
FROM transactions_info
JOIN customer_info USING(id_client) 
GROUP BY gender;

# % соотношение полов в каждом месяце с их долей затрат
SELECT date_new, gender, 
		COUNT(id_client) as count_clients,
        ROUND(COUNT(id_client) / (SELECT COUNT(id_client) FROM transactions_info) * 100, 2) as percent_clients,
        SUM(sum_payment) as total_spending,
        ROUND(SUM(sum_payment) / (SELECT SUM(sum_payment) FROM transactions_info) * 100, 2) as percent_spending
FROM transactions_info
JOIN customer_info USING(id_client)
GROUP BY date_new, gender
ORDER BY date_new, gender DESC;


# ЗАДАНИЕ 3
# информация о возрастных группах клиентов - их сумма затрат и кол-во операции за весь период
WITH clients_group AS (
SELECT id_client, age,
	CASE
		WHEN age < 10 THEN 'Возраст 0-10 лет'
        WHEN age < 20 THEN 'Возраст 10-20 лет'
        WHEN age < 30 THEN 'Возраст 20-30 лет'
        WHEN age < 40 THEN 'Возраст 30-40 лет'
        WHEN age < 50 THEN 'Возраст 40-50 лет'
        WHEN age < 60 THEN 'Возраст 50-60 лет'
        WHEN age < 70 THEN 'Возраст 60-70 лет'
        WHEN age < 80 THEN 'Возраст 70-80 лет'
        WHEN age < 90 THEN 'Возраст 80-90 лет'
        WHEN age < 100 THEN 'Возраст 90-100 лет'
        ELSE 'Возраст неопределен'
	END AS age_groups,
    SUM(sum_payment) as total_sum,
    COUNT(DISTINCT id_check) as operations_per_client
FROM customer_info
JOIN transactions_info USING(id_client)
GROUP BY id_client, age 
)

SELECT age_groups, SUM(total_sum) as total_spending, COUNT(operations_per_client) as total_operations FROM clients_group
GROUP BY age_groups
ORDER BY age_groups;

# поквартальная информация - средняя сумма затрат, среднее кол-во операций и их процентаж
WITH quarter_info AS (
SELECT date_new, 
	CASE
		WHEN month(date_new) <= 3 THEN 'Первый квартал'
        WHEN month(date_new) <= 6 THEN 'Второй квартал'
        WHEN month(date_new) <= 9 THEN 'Третий квартал'
        WHEN month(date_new) <= 12 THEN 'Четвертый квартал'
    END as quarter,
    SUM(sum_payment) as spending_sum,
    COUNT(DISTINCT id_check) as count_operations
FROM transactions_info
GROUP BY date_new, quarter
ORDER BY date_new, quarter
)

SELECT quarter, 
		ROUND(AVG(spending_sum), 2) as avg_spending,
		ROUND(AVG(spending_sum) / (SELECT SUM(sum_payment) FROM transactions_info) * 100, 2) as percent_spending,
		ROUND(AVG(count_operations), 2) as avg_operation,
        ROUND(AVG(count_operations) / (SELECT COUNT(DISTINCT id_check) FROM transactions_info) * 100, 2) as percent_operations
FROM quarter_info
GROUP BY quarter
ORDER BY quarter;