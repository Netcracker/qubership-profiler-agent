-- create metadata tables

CREATE TABLE IF NOT EXISTS pods
(
    namespace    text,
    service_name text,
    pod_name     text,
    active_since timestamptz,
    last_restart timestamptz,
    last_active  timestamptz,
    tags         jsonb,
    pod_id       text, -- id (ns.service.pod) as FK for other tables
    PRIMARY KEY (namespace, service_name, pod_name)
);
CREATE INDEX IF NOT EXISTS pods_ns_idx ON pods (namespace);
CREATE INDEX IF NOT EXISTS pods_id_idx ON pods (pod_id);

CREATE TABLE IF NOT EXISTS pod_restarts
(
    pod_id       text,
    namespace    text,
    service_name text,
    pod_name     text,
    restart_time timestamptz,
    active_since timestamptz,
    last_active  timestamptz,
    PRIMARY KEY (namespace, service_name, pod_name, restart_time)
    -- PRIMARY KEY (pod_id)
);
CREATE INDEX IF NOT EXISTS pod_restarts_id_idx ON pod_restarts (pod_id); -- TODO create unique index, read about it 
CREATE INDEX IF NOT EXISTS pod_restarts_ns_idx ON pod_restarts (namespace, service_name);

-- TODO сделать index по pod_name + restart_time
CREATE TABLE IF NOT EXISTS dictionary
(
    pod_id       text,
    pod_name     text,
    restart_time timestamptz,
    position     integer,
    tag          text,
    PRIMARY KEY (pod_name, restart_time, position)
);

-- TODO сделать index по pod_name + restart_time
CREATE TABLE IF NOT EXISTS params
(
    pod_id       text,
    pod_name     text,
    restart_time timestamptz,
    param_name   text,
    param_index  boolean,
    param_list   boolean,
    param_order  integer,
    signature    text,
    PRIMARY KEY (pod_name, restart_time, param_name)
);

-- TODO проанализировать, надо ли здесь сделать партиционирование по дням
-- TODO сделать index по date и по pod_name + restart_time
CREATE TABLE IF NOT EXISTS pod_statistics
(
    date                 timestamptz,
    pod_id               text,
    pod_name             text,
    restart_time         timestamptz,
    cur_time             timestamptz, -- by minute
    data_accumulated     jsonb,
    original_accumulated jsonb,
    PRIMARY KEY (date, pod_name, restart_time, cur_time)
);
