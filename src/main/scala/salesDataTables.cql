drop table amount_spent_agg;
drop table agg_summary;
drop table customers_by_state;
drop table total_sales_aug;
drop table total_sales_by_month;

create table amount_spent_agg(customer_id int,
sum_amount double,
PRIMARY KEY((customer_id), sum_amount))
WITH CLUSTERING ORDER BY (sum_amount DESC);

create table agg_summary(total_customers int,
total_spent double,
max_spent double,
least_spent double,
avg_spent double,
mean_spent double,
skewness double,
PRIMARY KEY(total_customers));

create table customers_by_state(state text,
count double,
PRIMARY KEY((state), count))
WITH CLUSTERING ORDER BY (count DESC);

create table total_sales_aug(sum_amount double,
PRIMARY KEY(sum_amount));

create table total_sales_by_month(unique_id int,
year int,
month int,
total_sales double,
PRIMARY KEY((unique_id), year, month))
WITH CLUSTERING ORDER BY (year ASC, month ASC);

