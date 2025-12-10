-- change context
use role sysadmin;
use schema dev_db.stage_sch;
use warehouse adhoc_wh;


-- create an internal stage and enable directory service
create stage if not exists raw_stg
directory = ( enable = true)
comment = 'all the air quality raw data will store in this internal stage location';


 -- create file format to process the JSON file
  create file format if not exists json_file_format 
      type = 'JSON'
      compression = 'AUTO' 
      comment = 'this is json file format object';


  show stages;
  list @raw_stg;

-- preview the data from stage with metadata columns
SELECT 
    t.$1 AS raw_content,
    REGEXP_SUBSTR(METADATA$FILENAME, 'country=([^/]+)', 1, 1, 'e', 1) AS country,
    REGEXP_SUBSTR(METADATA$FILENAME, 'city=([^/]+)', 1, 1, 'e', 1) AS city,
    REGEXP_SUBSTR(METADATA$FILENAME, 'district=([^/]+)', 1, 1, 'e', 1) AS district,
    CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=([0-9]{4})', 1, 1, 'e', 1) AS INT) AS year,
    CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'month=([0-9]{1,2})', 1, 1, 'e', 1) AS INT) AS month,
    CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'day=([0-9]{1,2})', 1, 1, 'e', 1) AS INT) AS day,
    METADATA$FILENAME AS _stg_file_name,
    METADATA$FILE_LAST_MODIFIED as _stg_file_load_ts,
    METADATA$FILE_CONTENT_KEY as _stg_file_md5,
    CURRENT_TIMESTAMP() AS _copy_data_ts,
    CURRENT_USER() AS _copy_data_user,
    CURRENT_ROLE() AS _copy_data_role
from @dev_db.stage_sch.raw_stg
(file_format => JSON_FILE_FORMAT) t;

-- create the raw table to land the data
CREATE OR REPLACE TRANSIENT TABLE raw_aqi (
    raw                      VARIANT,
    country                  VARCHAR,
    city                     VARCHAR,
    district                 VARCHAR,
    year                     NUMBER(4,0),
    month                    NUMBER(2,0),
    day                      NUMBER(2,0),
    _stg_file_name           VARCHAR,
    _stg_file_load_ts        TIMESTAMP_NTZ,
    _stg_file_md5            VARCHAR,
    _copy_data_ts            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _copy_data_user          VARCHAR DEFAULT CURRENT_USER(),
    _copy_data_role          VARCHAR DEFAULT CURRENT_ROLE()
);

-- following copy command
create or replace task copy_air_quality_data
    warehouse = load_wh
    schedule = 'USING CRON 0 * * * * America/Lima'
as
copy into raw_aqi (raw, country, city, district, year, month, day, _stg_file_name, _stg_file_load_ts, _stg_file_md5) from 
(
SELECT 
        t.$1 AS raw,
        REGEXP_SUBSTR(METADATA$FILENAME, 'country=([^/]+)', 1, 1, 'e', 1) AS country,
        REGEXP_SUBSTR(METADATA$FILENAME, 'city=([^/]+)', 1, 1, 'e', 1) AS city,
        REGEXP_SUBSTR(METADATA$FILENAME, 'district=([^/]+)', 1, 1, 'e', 1) AS district,
        CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'year=([0-9]{4})', 1, 1, 'e', 1) AS INT) AS year,
        CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'month=([0-9]{1,2})', 1, 1, 'e', 1) AS INT) AS month,
        CAST(REGEXP_SUBSTR(METADATA$FILENAME, 'day=([0-9]{1,2})', 1, 1, 'e', 1) AS INT) AS day,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED as _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY as _stg_file_md5,
    from @dev_db.stage_sch.raw_stg
    (file_format => JSON_FILE_FORMAT) t
)
file_format = (format_name = 'dev_db.stage_sch.JSON_FILE_FORMAT') 
ON_ERROR = ABORT_STATEMENT; 


use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

alter task dev_db.stage_sch.copy_air_quality_data resume;

-- check the data
select *
    from raw_aqi
    limit 10;

-- select with ranking
select 
    index_record_ts,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5 ,_copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from raw_aqi 
order by index_record_ts desc
limit 10;