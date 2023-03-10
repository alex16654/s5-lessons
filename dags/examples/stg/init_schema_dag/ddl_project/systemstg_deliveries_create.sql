CREATE TABLE IF NOT EXISTS de.stg.system_deliveries(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT system_deliveries_pk PRIMARY KEY (id),
    CONSTRAINT system_deliveries_object_id_uique UNIQUE (object_id));
