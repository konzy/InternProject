create keyspace sales_data with replication = {'class': 'SimpleStrategy' , 'replication_factor': 1};

use keyspace sales_data;

create table amount_spent_agg(customer_id int,
sum_amount double,
PRIMARY KEY(customer_id));

create table customer_ordered_most(customer_id int,
max_sum_amount double,
PRIMARY KEY(customer_id));

create table most_customers_by_state(best_state text,
max_count double,
PRIMARY KEY(best_state));

create table total_sales_aug(sum_amount double,
PRIMARY KEY(sum_amount));

create table total_sales_by_month(year Int,
month Int,
total_sales double,
PRIMARY KEY(year, month))
