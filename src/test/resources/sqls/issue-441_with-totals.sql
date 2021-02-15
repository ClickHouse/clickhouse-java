WITH 2 AS factor
SELECT 
    number % 2 AS odd_even, 
    count(*) AS count, 
    sum(factor * number) AS output
FROM 
(
    SELECT number
    FROM system.numbers
    LIMIT 100
)
GROUP BY number % 2
    WITH TOTALS