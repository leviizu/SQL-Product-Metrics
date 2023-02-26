--Engagement Score
SELECT
  allspacesanddates.space_id,
  allspacesanddates.event_date,
  allspacesanddates.event_hour,
  numberoflikes,
  NumberOfComments,
  numberofEventsJoined,
  numberOfPosts,
  numberOfChatsSent
FROM ((
      --extracting all dates and spaces linked to targeted events so as to have unique values
    SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour
      FROM
        `office-life-169414.analytics_157635370.events_*`
      CROSS JOIN
        UNNEST (user_properties)
      WHERE
        key = 'space_id'
        AND event_name= 'post_liked')
    UNION DISTINCT (
       SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour
      FROM
        `office-life-169414.analytics_157635370.events_*`
      CROSS JOIN
        UNNEST (user_properties)
      WHERE
        key = 'space_id'
        AND event_name= 'post_commented')
    UNION DISTINCT (
      SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour
      FROM
        `office-life-169414.analytics_157635370.events_*`
      CROSS JOIN
        UNNEST (user_properties)
      WHERE
        key = 'space_id'
        AND event_name= 'event_joined')
    UNION DISTINCT (
      SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour
      FROM
        `office-life-169414.analytics_157635370.events_*`
      CROSS JOIN
        UNNEST (user_properties)
      WHERE
        key = 'space_id'
        AND event_name= 'post_published')
    UNION DISTINCT (
      SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour
      FROM
        `office-life-169414.analytics_157635370.events_*`
      CROSS JOIN
        UNNEST (user_properties)
      WHERE
        key = 'space_id'
        AND event_name= 'chat_message_sent'))allspacesanddates
    --joining the spaces and dates to count specific events
  FULL JOIN (
    SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour,
      COUNT(event_name) AS numberoflikes,
    FROM
      `office-life-169414.analytics_157635370.events_*`
    CROSS JOIN
      UNNEST (user_properties)
    WHERE
      key = 'space_id'
      AND event_name= 'post_liked'
    GROUP BY
      event_date,
      event_hour,
      value.string_value)post_liked
  ON
    allspacesanddates.space_id=post_liked.space_id
    AND allspacesanddates.event_date=post_liked.event_date
    AND allspacesanddates.event_hour=post_liked.event_hour
  FULL JOIN (
    SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour,
      COUNT(event_name) AS NumberOfComments,
    FROM
      `office-life-169414.analytics_157635370.events_*`
    CROSS JOIN
      UNNEST (user_properties)
    WHERE
      key = 'space_id'
      AND event_name= 'post_commented'
    GROUP BY
      event_date,
      event_hour,
      value.string_value)post_commented
  ON
    allspacesanddates.space_id=post_commented.space_id
    AND allspacesanddates.event_date=post_commented.event_date
    AND allspacesanddates.event_hour=post_commented.event_hour
  FULL JOIN (
     SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour,
      COUNT(event_name) AS numberofEventsJoined,
    FROM
      `office-life-169414.analytics_157635370.events_*`
    CROSS JOIN
      UNNEST (user_properties)
    WHERE
      key = 'space_id'
      AND event_name= 'event_joined'
    GROUP BY
      event_date,
      event_hour,
      value.string_value)ej
  ON
    allspacesanddates.space_id=ej.space_id
    AND allspacesanddates.event_date=ej.event_date
    AND allspacesanddates.event_hour=ej.event_hour
  FULL JOIN (
     SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour,
      COUNT(event_name) AS numberOfPosts,
    FROM
      `office-life-169414.analytics_157635370.events_*`
    CROSS JOIN
      UNNEST (user_properties)
    WHERE
      key = 'space_id'
      AND event_name= 'post_published'
    GROUP BY
      event_date,
      event_hour,
      value.string_value)pp
  ON
    allspacesanddates.space_id=pp.space_id
    AND allspacesanddates.event_date=pp.event_date
    AND allspacesanddates.event_hour=pp.event_hour
  FULL JOIN (
    SELECT
      value.string_value AS space_id,
      CAST( CONCAT( SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2) ) AS DATE) AS event_date,
      LPAD(CAST (EXTRACT(HOUR
            FROM
              TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') AS event_hour,
      COUNT(event_name) AS numberOfChatsSent,
    FROM
      `office-life-169414.analytics_157635370.events_*`
    CROSS JOIN
      UNNEST (user_properties)
    WHERE
      key = 'space_id'
      AND event_name= 'chat_message_sent'
    GROUP BY
      event_date,
      event_hour,
      value.string_value)cms
  ON
    allspacesanddates.space_id=cms.space_id
    AND allspacesanddates.event_date=cms.event_date
    AND allspacesanddates.event_hour=cms.event_hour

