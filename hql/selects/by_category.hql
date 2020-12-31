-- REPORT BY CATEGORY --

SELECT i.item_category_id as item_category_id, i.item_category_name as item_category_name, sum(s.item_price * s.item_count_day) AS sales, sum(s.item_count_day) AS items_sold
FROM sales s
JOIN items i ON i.item_id=s.item_id
WHERE record_date=${current_date}
GROUP BY i.item_category_id, i.item_category_name SORT BY sales desc;
