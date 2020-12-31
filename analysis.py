from pyspark.sql import Window
from pyspark.sql import functions as F

# ABC XYZ analysis

spark.sql('use zakhaand')
sales_by_items = spark.sql(
    'select item_id, sum(item_price*item_count_day) as sales, sum(item_count_day) as items_sold from '
    'sales group by item_id order by sales desc')
totals = sales_by_items.agg(F.sum('sales'), F.sum('items_sold')).collect()
total_sales = totals[0][0]
total_sold = totals[0][1]

sales_percentages = sales_by_items.withColumn('proportion_of_sales', F.col('sales') / total_sales) \
    .withColumn('proportion_of_sold', F.col('items_sold') / total_sold) \
    .withColumn('cumulative_prop_sold', F.sum('proportion_of_sales').over(Window.orderBy(F.col('sales').desc())))

var_coef = spark \
    .sql('select month,item_id,sum(item_count_day) as items_sold from sales group by item_id, month') \
    .groupBy('item_id') \
    .agg(F.avg('items_sold').alias('avg_items_sold'), F.stddev_pop('items_sold').alias('stddev_items_sold')) \
    .withColumn('var_coef', F.col('stddev_items_sold') / F.col('avg_items_sold')) \
    .drop('avg_items_sold', 'stddev_items_sold')
abc_xyz = sales_percentages.join(var_coef, sales_by_items.item_id == var_coef.item_id, how='left') \
    .select(sales_percentages['*'], var_coef['var_coef'])
abc_xyz = abc_xyz.withColumn('ABC', F.when(F.col('cumulative_prop_sold') <= 0.70, 'A')
                             .when((F.col('cumulative_prop_sold') > 0.70) & (F.col('cumulative_prop_sold') < 0.95), 'B')
                             .otherwise('C'))
abc_xyz = abc_xyz.withColumn('XYZ', F.when(F.col('var_coef') <= 0.1, 'X')
                             .when((F.col('var_coef') > 0.1) & (F.col('var_coef') < 0.25), 'Y')
                             .otherwise('Z')).cache()

abc_xyz_table = abc_xyz.groupBy('ABC', 'XYZ') \
    .agg(F.sum('sales').alias('total_sales'), F.sum('proportion_of_sales').alias('proportion_of_sales'),
         F.sum('proportion_of_sold').alias('prop_of_sold'), F.count('*').alias('number_of_items')) \
    .orderBy('ABC', 'XYZ')
abc_xyz_table.coalesce(1).write.mode('overwrite').csv('semestral-project/results/abc_xyz_table', header=True)
# show examples
items = spark.sql('select * from items')
abc_xyz_items = abc_xyz.join(items, items.item_id == abc_xyz.item_id)
win = Window.partitionBy('ABC', 'XYZ').orderBy('ABC', 'XYZ')
examples = abc_xyz_items.withColumn('rn', F.row_number().over(win)) \
    .where('rn<3').select('ABC', 'XYZ', 'item_name', 'item_category_name').orderBy('ABC', 'XYZ')
examples.coalesce(1).write.mode('overwrite').csv('semestral-project/results/abc_xyz_examples', header=True)
### EXPLORATORY ANALYSIS ###

# 1) Average top 10 prices
avg_price = spark.sql('select item_id, avg(item_price) as avg_item_price from sales group by item_id '
                      'order by avg_item_price desc')
avg_price.coalesce(1).write.mode('overwrite').csv('semestral-project/results/avg_price', header=True)

top_10_avg_price = avg_price.join(items, avg_price.item_id == items.item_id) \
    .select(avg_price['*'], items['item_name']).orderBy(F.col('avg_item_price').desc()).limit(10)
top_10_avg_price.coalesce(1).write.mode('overwrite').csv('semestral-project/results/top_10_avg_price', header=True)

# 2) Top 10 products by top 5 biggest categories
sales = spark.sql('select * from sales')
sales_items = sales.join(items, sales.item_id == items.item_id) \
    .select(sales['*'], items['item_name'], items['item_category_id'], items['item_category_name']) \
    .cache()
categories_stats = sales_items.groupBy('item_category_id', 'item_category_name') \
    .agg(F.avg('item_price').alias('avg_item_price'), F.sum('item_count_day').alias('items_sold')).cache()
biggest_5_cats = categories_stats.orderBy(F.col('items_sold').desc()).limit(5)
biggest_5_cats.coalesce(1).write.mode('overwrite').csv('semestral-project/results/biggest_5_cats', header=True)

average_prices_by_5_biggest_cats = sales_items.filter('item_category_id in (40, 30,55,19,37)') \
    .groupBy('item_id', 'item_category_id', 'item_category_name') \
    .agg(F.avg('item_price').alias('avg_item_price'))
average_prices_by_5_biggest_cats.coalesce(1).write.mode('overwrite') \
    .csv('semestral-project/results/avg_price_biggest_5_cats', header=True)

sales_items.unpersist()

# 3) Top 10 categories by avg price
avg_price_by_cats = sales_items.groupBy('item_category_id') \
    .agg(F.avg('item_price').alias('avg_item_price'))
avg_price_by_cats.coalesce(1).write.mode('overwrite') \
    .csv('semestral-project/results/avg_price_by_cats', header=True)

top_10_cats = categories_stats.orderBy(F.col('avg_item_price').desc()).limit(10)
top_10_cats.write.mode('overwrite').csv('semestral-project/results/top_10_cats', header=True)

categories_stats.unpersist()
# 4) Top 10 by AX,BX,CZ
top_10_ax = abc_xyz.filter('ABC="A" and XYZ="X"') \
    .orderBy(F.col('sales').desc()).limit(10) \
    .select(abc_xyz['item_id'], abc_xyz['sales'], abc_xyz['ABC'], abc_xyz['XYZ'])
top_10_ax = top_10_ax.join(items, items.item_id == top_10_ax.item_id, how='left') \
    .select(top_10_ax['*'], items['item_name'])
top_10_ax.write.mode('overwrite').csv('semestral-project/results/top_10_ax', header=True)

top_10_bx = abc_xyz.filter('ABC="B" and XYZ="X"') \
    .orderBy(F.col('sales').desc()).limit(10) \
    .select(abc_xyz['item_id'], abc_xyz['sales'], abc_xyz['ABC'], abc_xyz['XYZ'])
top_10_bx = top_10_bx.join(items, items.item_id == top_10_bx.item_id, how='left') \
    .select(top_10_bx['*'], items['item_name'])
top_10_bx.write.mode('overwrite').csv('semestral-project/results/top_10_bx', header=True)

top_10_cz = abc_xyz.filter('ABC="C" and XYZ="Z"') \
    .orderBy(F.col('sales').desc()).limit(10) \
    .select(abc_xyz['item_id'], abc_xyz['sales'], abc_xyz['ABC'], abc_xyz['XYZ'])
top_10_cz = top_10_cz.join(items, items.item_id == top_10_cz.item_id, how='left') \
    .select(top_10_cz['*'], items['item_name'])
top_10_cz.write.mode('overwrite').csv('semestral-project/results/top_10_cz', header=True)

# 5) Time graphs of sales
sales_by_day = spark.sql('select record_date, sum(item_price*item_count_day) as sales from sales group by record_date '
                         'order by record_date')
sales_by_day.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_day', header=True)

sales = spark.sql('select * from sales').cache()
sales_w_week = sales.withColumn('week_of_year', F.weekofyear(sales.record_date))
sales_by_week = sales_w_week.groupBy('year', 'week_of_year') \
    .agg(F.sum(sales_w_week.item_price * sales_w_week.item_count_day).alias('sales_per_week')) \
    .withColumn('year-week', F.concat(F.col('year'), F.lit('-'), F.col('week_of_year'))) \
    .orderBy('year', 'week_of_year')
sales_by_week.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_week', header=True)

sales_by_month = sales.groupBy('year', 'month') \
    .agg(F.sum(sales_w_week.item_price * sales_w_week.item_count_day).alias('sales_by_month')) \
    .withColumn('year-month', F.concat(F.col('year'), F.lit('-'), F.col('month'))) \
    .orderBy('year', 'month')
sales_by_month.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_month', header=True)
# 6) Hist By days
sales_w_days = sales_w_week.withColumn('day_of_week', F.dayofweek(sales_w_week.record_date)) \
    .filter("year < 2015").cache()

sales_by_days_of_week = sales_w_days.groupBy('day_of_week') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')) \
    .orderBy('day_of_week')
sales_by_days_of_week.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_days_of_week',
                                                              header=True)

sales_by_days_of_month = sales_w_days.groupBy('day') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')).orderBy('day')
sales_by_days_of_month.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_days_of_month',
                                                               header=True)

sales_by_week_of_year = sales_w_days.groupBy('week_of_year') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')).orderBy('week_of_year')
sales_by_week_of_year.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_week_of_year',
                                                              header=True)

sales_by_month_of_year = sales_w_days.groupBy('month') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')).orderBy('month')
sales_by_month_of_year.coalesce(1).write.mode('overwrite').csv('semestral-project/results/sales_by_month_of_year',
                                                               header=True)
# 7)

sales_w_days_cats = sales_w_days.join(items, sales_w_days.item_id == items.item_id) \
    .select(sales_w_days['*'], items.item_category_id) \
    .filter('item_category_id in (40, 30,55,19,37)')

sales_5_cats_by_days_of_week = sales_w_days_cats.groupBy('day_of_week', 'item_category_id') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')) \
    .orderBy('day_of_week')
sales_5_cats_by_days_of_week.coalesce(1).write.mode('overwrite') \
    .csv('semestral-project/results/sales_5_cats_by_days_of_week', header=True)

sales_5_cats_by_days_of_month = sales_w_days_cats.groupBy('day', 'item_category_id') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')).orderBy('day')
sales_5_cats_by_days_of_month.coalesce(1).write.mode('overwrite') \
    .csv('semestral-project/results/sales_5_cats_by_days_of_month', header=True)

sales_5_cats_by_week_of_year = sales_w_days_cats.groupBy('week_of_year', 'item_category_id') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')).orderBy('week_of_year')
sales_5_cats_by_week_of_year.coalesce(1).write.mode('overwrite') \
    .csv('semestral-project/results/sales_5_cats_by_week_of_year', header=True)

sales_5_cats_by_month_of_year = sales_w_days_cats.groupBy('month', 'item_category_id') \
    .agg(F.sum(sales_w_days.item_price * sales_w_days.item_count_day).alias('sales')).orderBy('month')
sales_5_cats_by_month_of_year.coalesce(1).write.mode('overwrite') \
    .csv('semestral-project/results/sales_5_cats_by_month_of_year', header=True)
