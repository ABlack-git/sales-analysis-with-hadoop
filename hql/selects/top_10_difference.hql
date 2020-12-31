-- TOP 10 DIFFERENCE --

SELECT diff.item_id as item_id, diff.daily_sold_diff as daily_difference, i.item_name as item_name FROM
(SELECT today.item_id, coalesce(today.item_sold,0) - coalesce(yesterday.item_sold,0) as daily_sold_diff FROM
(SELECT item_id, sum(item_count_day) as item_sold
FROM sales
WHERE record_date=${current_date}
GROUP BY item_id) as today
FULL JOIN
(SELECT item_id, sum(item_count_day) as item_sold
FROM sales
WHERE record_date=date_sub(${current_date}, 1)
GROUP BY item_id) as yesterday
ON today.item_id = yesterday.item_id) diff
JOIN items i ON i.item_id=diff.item_id
ORDER BY abs(diff.daily_sold_diff) desc LIMIT 10;