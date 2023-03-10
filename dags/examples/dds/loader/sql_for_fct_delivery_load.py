SQL_FOR_FCT_DELIVERY_LOAD = """

insert into dds.dm_deliveries (
    delivery_id, 
    courier_id, 
    order_id, 
    order_ts, 
    delivery_ts, 
    address, 
    rate, 
    tip_sum, 
    total_sum
)
WITH w AS  
(
SELECT  
            replace(replace(object_value, '"', ''), '''','"')::JSON as val
FROM  		stg.system_deliveries 
)
SELECT  
            val->>'delivery_id' as delivery_id,
            dc.id as courier_id,
            dor.id as order_id,
            (val->>'order_ts')::timestamptz as order_ts,
            (val->>'delivery_ts')::timestamptz as delivery_ts,
            val->>'address' as address,
            (val->>'rate')::integer as rate,
            (val->>'tip_sum')::DECIMAL  as tip_sum,
            (val->>'sum')::DECIMAL as total_sum
FROM  		w
LEFT  JOIN  	dds.dm_couriers dc ON val->>'courier_id' = dc.courier_id
LEFT  JOIN  	dds.dm_orders dor ON val->>'order_id' = dor.order_key
WHERE val->>'delivery_id' NOT IN (SELECT DISTINCT delivery_id FROM dds.dm_deliveries);

"""
