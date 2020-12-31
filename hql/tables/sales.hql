-- CREATE EXTERNAL TABLE --

create external table if not exists sales_ext
(
record_date string,
date_block_num int,
shop_id bigint,
item_id bigint,
item_price float,
item_count_day int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/user/zakhaand/semestral-project/sales'
tblproperties ("skip.header.line.count"="1");

-- CREATE MANAGED TABLE --

create table if not exists sales
(
record_date string,
date_block_num int,
shop_id bigint,
item_id bigint,
item_price float,
item_count_day int,
year int,
day int
)
partitioned by (month int)
stored as parquet
tblproperties("parquet.compress"="SNAPPY");

-- INSERT DATA TO MANAGED TABLE --
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table sales
partition(month)
select
from_unixtime(unix_timestamp(record_date, 'dd.MM.yyyy'), 'yyyy-MM-dd'),
date_block_num,
shop_id,
item_id,
item_price,
item_count_day,
year(from_unixtime(unix_timestamp(record_date, 'dd.MM.yyyy'), 'yyyy-MM-dd')),
day(from_unixtime(unix_timestamp(record_date, 'dd.MM.yyyy'), 'yyyy-MM-dd')),
month(from_unixtime(unix_timestamp(record_date, 'dd.MM.yyyy'), 'yyyy-MM-dd'))
from sales_ext;