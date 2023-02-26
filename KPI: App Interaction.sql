SELECT
  all_spaces.space_id,
  os.users AS os_users,
  os.os AS os,
  lang.users AS language_users,
  lang.device_language
FROM (--extracting all distinct spaces in database
  SELECT
    DISTINCT value.string_value AS space_id
  FROM
    `database1*`
  CROSS JOIN
    UNNEST (user_properties)
  WHERE
    key = 'space_id') all_spaces
LEFT JOIN (--extracting users by their operating system
  SELECT
    value.string_value AS space_id,
    COUNT(DISTINCT user_id) AS Users,
    device.operating_system AS os
  FROM
    `database1*`
  CROSS JOIN
    UNNEST (user_properties)
  WHERE
    key = 'space_id'
  GROUP BY
    space_id,
    os)os
ON
  all_spaces.space_id=os.space_id
LEFT JOIN (--extracting users by device language
  SELECT
    value.string_value AS space_id,
    COUNT(DISTINCT user_id) AS Users,
    SUBSTR(device.language,0,2) AS device_language
  FROM
    `database1*`
  CROSS JOIN
    UNNEST (user_properties)
  WHERE
    key = 'space_id'
  GROUP BY
    space_id,
    device_language)lang
ON
  all_spaces.space_id=lang.space_id
