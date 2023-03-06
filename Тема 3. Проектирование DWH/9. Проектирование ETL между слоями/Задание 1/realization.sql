INSERT INTO cdm.dm_settlement_report(
       restaurant_id,
       restaurant_name,
       settlement_date,
       orders_count,
       orders_total_sum,
       orders_bonus_payment_sum,
       orders_bonus_granted_sum,
       order_processing_fee,
       restaurant_reward_sum)
SELECT do2.restaurant_id,
       dr.restaurant_name,
       dt."date"::date,
       count(DISTINCT fps.order_id),
       sum(fps.total_sum),
       sum(fps.bonus_payment),
       sum(fps.bonus_grant),
       (sum(fps.total_sum) * 0.25) AS order_processing_fee,
       sum(fps.total_sum) -  sum(fps.total_sum) * 0.25 - sum(fps.bonus_payment) AS restaurant_reward_sum
FROM dds.fct_product_sales fps
JOIN dds.dm_orders do2 ON fps.order_id = do2.id
JOIN dds.dm_restaurants dr ON do2.restaurant_id = dr.id
JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
WHERE do2.order_status = 'CLOSED'
GROUP BY 1,2,3
ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET restaurant_id = EXCLUDED.restaurant_id|| ':' || cdm.dm_settlement_report.restaurant_id;
