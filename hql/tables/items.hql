-- CREATE EXTERNAL TABLE FROM ITEMS --

create external table if not exists items_ext
(
item_name string,
item_id bigint,
item_category_id bigint
)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
location '/user/zakhaand/semestral-project/items'
tblproperties ("skip.header.line.count"="1");

-- CREATE EXTERNAL TABLE FROM ITEMS_CAT --

create external table if not exists items_cat_ext
(
item_category_name string,
item_category_id bigint
)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
location '/user/zakhaand/semestral-project/items-cat'
tblproperties ("skip.header.line.count"="1");

-- CREATE MANAGED TABLES ITEMS --

create table if not exists items
(
item_name string,
item_id bigint,
item_category_id bigint,
item_category_name string
)
stored as parquet
tblproperties("parquet.compress"="SNAPPY");

-- INSERT TO MANAGED TABLE --

insert overwrite table items
select it.item_name, it.item_id, it.item_category_id, itc.item_category_name
from items_ext it
join items_cat_ext itc
on it.item_category_id=itc.item_category_id;