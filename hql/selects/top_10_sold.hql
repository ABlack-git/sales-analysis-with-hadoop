-- TOP 10 BY SOLD --

SELECT s.item_id AS item_id, sum(s.item_price * s.item_count_day) AS sales, sum(s.item_count_day) AS items_sold, i.item_name as item_name
FROM sales s
JOIN items i ON i.item_id=s.item_id
WHERE record_date=${current_date}
GROUP BY s.item_id, i.item_name ORDER BY items_sold desc limit 10;
