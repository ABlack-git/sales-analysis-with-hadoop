TODAY_DATE="\"2015-10-31\""

# Update or create tables
echo "Updating tables..."
beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/zakhaand;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
  -f hql/tables/sales.hql --silent=true

export HADOOP_CLIENT_OPTS="-Ddisable.quoting.for.sv=false"
# Produce daily analysis
echo "Producing daily by_category report..."
beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/zakhaand;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
  --hivevar current_date=${TODAY_DATE} -f hql/selects/by_category.hql --silent=true --verbose=false \
  --outputformat=csv2 >categories.csv

echo "Producing daily top_10_sold report..."
beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/zakhaand;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
  --hivevar current_date=${TODAY_DATE} -f hql/selects/top_10_sold.hql --silent=true --verbose=false \
  --outputformat=csv2 >top_10_sold.csv

echo "Producing daily top_10_sales report..."
beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/zakhaand;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
  --hivevar current_date=${TODAY_DATE} -f hql/selects/top_10_sales.hql --silent=true --verbose=false \
  --outputformat=csv2 >top_10_sales.csv

echo "Producing daily top_10_difference report..."
beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/zakhaand;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ" \
  --hivevar current_date=${TODAY_DATE} -f hql/selects/top_10_difference.hql --silent=true --verbose=false \
  --outputformat=csv2 >top_10_difference.csv
