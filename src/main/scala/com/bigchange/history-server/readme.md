# [Spark History Server](https://spark.apache.org/docs/latest/monitoring.html#spark-history-server-configuration-options)

Spark History Server is a web UI for viewing the progress of Spark jobs. It allows you to see how many tasks have been run, how much data has been processed and what the performance was like.

## 启动

```shell script
/bin/bash -c $SPARK_HOME/sbin/start-history-server.sh && tail -f /dev/null

```

