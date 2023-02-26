SELECT
  table4.space_id,
  table4.Event_Month AS EventMonth,
  table4.Totalsignups AS TotalSignups,
  table4.mau AS MonthlyActiveUsers,
  CASE
    WHEN table4.Totalsignups IS NULL THEN table4.mau/table4.mau
  ELSE
  round ((table4.mau/table4.Totalsignups),
    2)
END
  AS MAUpercent
FROM (
--replacing number of already active per month with previous value in case value is NULL

  SELECT
    table3.space_id AS space_id,
    table3.eventmonth AS Event_Month,
    table3.mau AS mau,
    table3.Totalsignups AS signup,
    FIRST_VALUE(table3.totalsignups) OVER (PARTITION BY table3.list, table3.space_id ORDER BY eventmonth) AS Totalsignups,
  FROM (
  
    SELECT
      table2.Space_id AS space_id,
      table2.Totalsignups AS totalsignups,
      table2.EventMonth AS eventmonth,
      table2.MonthlyActiveUsers AS mau,
      COUNT(table2.Totalsignups) OVER (PARTITION BY table2.Space_id ORDER BY table2.EventMonth) AS list
    FROM (
      SELECT
        mau.space_id AS Space_id,
        signup.Totalsignups AS Totalsignups,
        mau.mau AS MonthlyActiveUsers,
        mau.EventMonth AS EventMonth
      FROM (
	  --determining the running sum of users already active in the platform at a perticular month
        SELECT
          table1.space_id,
          table1.signupmonth AS signupmonth,
          SUM(table1.signups) OVER (PARTITION BY table1.space_id ORDER BY table1.signupmonth ) AS Totalsignups
        FROM (
          SELECT
            added.space_id AS space_id,
            added.signupmonth AS signupmonth,
            COUNT(DISTINCT user_id) AS signups
          FROM (
            (SELECT
              value.string_value AS space_id,
              user_id,
			  --determining user's first activity date on the platform
              MIN(CONCAT(CAST (EXTRACT(YEAR
                    FROM
                      TIMESTAMP_MICROS(event_timestamp)) AS string),'-', LPAD(CAST (EXTRACT(MONTH
                      FROM
                        TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') )) AS signupmonth
            FROM
              `office-life-169414.analytics_157635370.events_*`
            CROSS JOIN
              UNNEST (user_properties)
            WHERE
              key = 'space_id'
            GROUP BY
              value.string_value,
              user_id)
			  --unioning old users by first activity of new database to that of old database
            UNION ALL
            (SELECT
              SUBSTR(onlycolumn, 0, 36) AS space_id,
              SUBSTR(onlycolumn, 38, 40) AS user_id,
              SUBSTR(onlycolumn, 79) AS signupmonth
            FROM
              `office-life-169414.analytics_157635370.old_db_imported_for_mau`
            WHERE
              onlycolumn != '_buildingId;_userId;signupmonth' ))added
          GROUP BY
            added.space_id,
            added.signupmonth )table1)signup
			--joining signuped users per month  to active users per month
      RIGHT JOIN (
        (SELECT
          value.string_value AS space_id,
          COUNT(DISTINCT user_id) AS mau,
          CONCAT(CAST (EXTRACT(YEAR
              FROM
                TIMESTAMP_MICROS(event_timestamp)) AS string),'-', LPAD(CAST (EXTRACT(MONTH
                FROM
                  TIMESTAMP_MICROS(event_timestamp)) AS string),2,'0') ) AS eventmonth
        FROM
          `office-life-169414.analytics_157635370.events_*`
        CROSS JOIN
          UNNEST (user_properties)
        WHERE
          key = 'space_id'
        GROUP BY
          value.string_value,
          eventmonth)
        UNION ALL
        (SELECT
          *
        FROM
          `office-life-169414.analytics_157635370.old_db_imported_for_mau_space_by_events` )) mau
      ON
        mau.space_id=signup.space_id
        AND mau.eventmonth=signup.signupmonth)table2)table3)table4
