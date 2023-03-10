CREATE TABLE IF NOT EXISTS de.stg.system_couriers(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT system_couriers_pk PRIMARY KEY (id),
    CONSTRAINT system_couriers_object_id_unique UNIQUE (object_id));