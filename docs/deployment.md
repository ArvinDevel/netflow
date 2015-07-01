## 部署

### 前期准备

#### 所有节点
1. /etc/hosts
2. 关闭防火墙
3. JDK-1.7的安装
4. snappy编译、安装
5. ganglia的gmond安装（可选）

#### master
1. 选择 _硬件条件好的节点_
2. 配置到所有其他节点的ssh免密码登录
3. ganglia的gmated和ganglia_web安装（可选）

### HDFS安装与配置
1. 2.4.0版本Hadoop
2. etc/hadoop/slaves
3. etc/hadoop/hdfs-site.xml
   - dfs.namenode.name.dir
   - dfs.datanode.data.dir
   - dfs.block.size
   - dfs.replication
   - dfs.socket.timeout
4. etc/hadoop/hadoop-env.sh
   - JAVA_HOME
5. etc/hadoo/hadoop-metrics.properties (可选)
6. 目录创建
   - /netflow
   - /spark-event-logs

### Spark安装配置
1. 1.5.0-NETFLOW版本Spark
2. dist/conf/slaves
3. dist/conf/spark-env.sh (可选)
4. dist/conf/log4j.properties (可选)
5. dist/conf/metrics.properties (可选)

### Netflow安装配置
1. 1.0-SNAPSHOT版本Netflow
2. conf/load-workers
3. conf/netflow-env.sh
	- NETFLOW_DAEMON_MEMORY
4. conf/spark-defaults.conf
	- spark.hadoop.fs.default.name
	- spark.eventLog.enabled
	- spark.eventLog.dir
	- spark.executor.memory
5. conf/netflow-defaults.conf
	- netflow.hadoop.fs.defaultFS
	- netflow.spark.master
	- netflow.spark.rest.master
	- netflow.parquet.compression
6. conf/log4j.properties
7. conf/metrics.properties (可选)

---

## 运行

### 集群启动

1. hadoop/sbin/start-dfs.sh
2. dist/sbin/start-all.sh (启动spark)
3. sbin/start-load-cluster.sh
4. sbin/start-query-master.sh
5. sbin/start-broker.sh --load-master netflow-load://ip:port netflow-query://ip:port


### 集群关闭
1. hadoop/sbin/stop-dfs.sh
2. dist/sbin/stop-all.sh (关闭spark)
3. sbin/stop-load-cluster.sh
4. sbin/stop-query-master.sh
5. sbin/stop-broker.sh
