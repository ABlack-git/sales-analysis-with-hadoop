-- TOP 10 BY SALES --

SELECT s.item_id as item_id, sum(s.item_price * s.item_count_day) AS sales, sum(s.item_count_day) AS items_sold, i.item_name as item_name
FROM sales s
JOIN items i ON i.item_id=s.item_id
WHERE record_date=${current_date}
GROUP BY s.item_id, i.item_name ORDER BY sales desc limit 10;
