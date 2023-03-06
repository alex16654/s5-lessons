WITH w as
(SELECT (event_value::JSON->>'product_payments')::JSON->>1 AS first_product_payment
FROM outbox),

w1 as
(SELECT first_product_payment
FROM w
WHERE first_product_payment IS NOT NULL)

SELECT DISTINCT first_product_payment::json->>'product_name' AS product_name
FROM w1;
