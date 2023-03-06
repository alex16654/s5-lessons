DROP TABLE IF EXISTS cdm.dm_settlement_report;

CREATE TABLE cdm.dm_settlement_report (
	id serial,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	--settlement_year int2 NOT NULL,
	--settlement_month int2 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
    CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
    CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01') AND (settlement_date < '2500-01-01'))));
