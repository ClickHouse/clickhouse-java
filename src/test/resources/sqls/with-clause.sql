WITH (
      (
          SELECT query_start_time_microseconds
          FROM system.query_log
          WHERE current_database = currentDatabase()
          ORDER BY query_start_time DESC
          LIMIT 1
      ) AS time_with_microseconds,
      (
          SELECT query_start_time
          FROM system.query_log
          WHERE current_database = currentDatabase()
          ORDER BY query_start_time DESC
          LIMIT 1
      ) AS t)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = 0, 'ok', 'fail')