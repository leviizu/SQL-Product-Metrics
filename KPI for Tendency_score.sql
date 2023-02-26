CREATE OR REPLACE VIEW
  tendency_score( USER_GROUP,
    ITEM,
    ID_NUMBER,
    COMPANY_NAME,
    PLATFORM,
    DATE,
    TYPE,
    OBJECT_NAME,
    INDEX_NUMBER ) AS --this is the first table
WITH
  position_calculation AS (
  SELECT
    Date,
    id_number,
    web_id,
    tracking_id,
    root_id MIN(position) AS true_position
  FROM
    position_table
  GROUP BY
    Date,
    id_number,
    web_id,
    tracking_id,
    root_id ),
  --this IS the second table
  Cte2 AS (
  SELECT
    u.user_group,
    i.item,
    p.id_number,
    co.company_name,
    tm.date,
    t.type,
    pf.platform,
    p.object_name CONCAT (‘-‘,g.geo1,’,’,g.geo2,’,’,g.geo3,’;’,
      CASE
        WHEN p.true_position IS NULL THEN ‘OUT OF scope’
      ELSE
      p.true_position
    END
      , '|',
      CASE
        WHEN sz.size IS NULL THEN ‘OUT OF scope’
      ELSE
      sz.size
    END
      , ':' ) AS index_number
  FROM
    position_calculation p
  LEFT JOIN
    users AS u
  ON
    p.id_number = u.id_number
  LEFT JOIN
    trackings AS tk
  ON
    p.id_number = tk.id_number
  LEFT JOIN
    platforms AS pf
  ON
    tk.platform_id = pf.platform_id
  LEFT JOIN
    companies AS co
  ON
    p.id_number = co.id_number
  LEFT JOIN
    types AS t
  ON
    p.id_number = t.id_number
  LEFT JOIN
    geo AS l
  ON
    tk.geo_id = g.geo_id
  LEFT JOIN
    sizes AS sz
  ON
    p.id_number = sz.id_number
  WHERE
    g.geo1 IS NOT NULL
    AND g.geo1<> '' )
  --this IS the final TABLE
SELECT
  user_group,
  item,
  id_number,
  company_name,
  platform,
  date,
  type,
  object_name,
  LISTAGG( DISTINCT index_number) AS index_number
FROM
  cte2
GROUP BY
  user_group,
  item,
  id_number,
  company_name,
  platform,
  date,
  type,
  object_name
