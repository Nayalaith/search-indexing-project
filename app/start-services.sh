#!/bin/bash

# starting HDFS daemons
$HADOOP_HOME/sbin/start-dfs.sh

# starting Yarn daemons
$HADOOP_HOME/sbin/start-yarn.sh

mapred --daemon start historyserver

hdfs dfsadmin -safemode leave

