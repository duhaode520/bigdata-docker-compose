# Big data playground: Cluster with Hadoop, Hive, Spark, Flink,HBase, Sqoop, Zeppelin and Livy via Docker-compose.

I wanted to have the ability to play around with various big data
applications as effortlessly as possible,
namely those found in Amazon EMR.
Ideally, that would be something that can be brought up and torn down
in one command. This is how this repository came to be!
## Development
有两个文件夹可以已经被挂载在集群中用于共享数据，`data_share` 只挂载在了 master 节点中，`data`挂载在了所有的 hadoop-spark 节点里。

在执行 `docker-compose up -d` 之前，最好先把 `docker-compose.yml` 中依赖的 image 都下下来，包括：
- postgres:11.5
- zookeeper
- duhaode520/bigdata_app:1.1
  > 这个包装起来会比较慢, 可以直接从这个 image 的 [dockerfile](https://github.com/duhaode520/hadoop-hive-hbase-spark-docker) 进行 build
- lisacumt/zeppelin-bigdata-docker:1.1.6

运行 ubuntu 版本的集群
```
docker-compose -f docker-compose.ubuntu.yml up -d
```

## ABOUT VERSION

HADOOP: 3.2.3

HIVE: 3.1.2

HBASE: 2.3.6

SPARK: 3.0.0

FLINK: 1.13.6

LIVY: 0.8.0 complied by myself

ZEPPELIN: 0.10.1

SQOOP: 1.4.7

TEZ: 0.9.2

SCALA: 2.12

Hudi: 0.10.0

Trino: 378  not full supporedt, becasue hadoop native packages not installed for apline reason.

## Constituent images:

fork from panovvv`s project,and add hbase service 

panovvv project url: https://github.com/panovvv/bigdata-docker-compose.git


## Usage

Clone:
```bash
git clone https://github.com/lschampion/bigdata-docker-compose.git
```
* On non-Linux platforms, you should dedicate more RAM to Docker than it does by default
  (docker run in centos8.4 and centos run in vmare15 sharing 8GB ram 60GB disk all on my machine with 16Gb RAM).  It seems that 16GB RAM of machine is minimum. Otherwise applications (ResourceManager in my case)
  will quit sporadically and you'll see messages like this one in logs:
  
  <pre>
  current-datetime INFO org.apache.hadoop.util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately 1234ms
  No GCs detected
  </pre>
Increasing memory to 8G solved all those mysterious problems for me.
  
* You should have more than 90% of free disk space, otherwise
  YARN will deem all nodes unhealthy.

Bring everything up:
```bash
cd bigdata-docker-compose
docker-compose up -d
```

* **data/** directory is mounted into every container, you can use this as
a storage both for files you want to process using Hive/Spark/whatever
and results of those computations.
* **livy_batches/** directory is where you have some sample code for
Livy batch processing mode. It's mounted to the node where Livy
is running. You can store your code there as well, or make use of the
universal **data/**.
* **zeppelin_notebooks/** contains, quite predictably, notebook files
for Zeppelin. Thanks to that, all your notebooks persist across runs.

Hive JDBC port is exposed to host:
* URI: `jdbc:hive2://localhost:10000`
* Driver: `org.apache.hive.jdbc.HiveDriver` (org.apache.hive:hive-jdbc:3.1.2)
* User and password: unused.

To shut the whole thing down, run this from the same folder:

Use with caution in `docker-compose down` command, because it remove container beyond retrieve.

```bash
docker-compose down
```

check docker-compose logs：

```shell
# for all aggregated logs 
docker-compose logs -f 
# for specific service 
docker-compose logs -f master
# for interested content 
docker-compose logs -f | grep your_interested_word
```

The logs of each component are synchronized to the data volume,please check dir ./logs

```shell
ll ./logs
# total 12
# drwxr-xr-x 3 root root 4096 Feb 27 23:12 hadoop
# drwxr-xr-x 2 root root 4096 Feb 27 23:13 hbase
# drwxr-xr-x 2 root root    6 Feb 27 13:32 hive
# drwxr-xr-x 2 root root 4096 Feb 27 23:13 spark
```

after fist startup, please use start / stop /restart. 

```
docker-compose start
docker-compose down
docker-compose restart
```

## Checking if everything plays well together

You can quickly check everything by opening the
[bundled Zeppelin notebook](http://localhost:8890)
and running all paragraphs.

Alternatively, to get a sense of
how it all works under the hood, follow the instructions below:

### Hadoop and YARN:

Check [YARN (Hadoop ResourceManager) Web UI
(localhost:8088)](http://localhost:8088/).
You should see 2 active nodes there.
There's also an
[alternative YARN Web UI 2 (http://localhost:8088/ui2)](http://localhost:8088/ui2).

Then, [Hadoop Name Node UI (localhost:9870)](http://localhost:9870),
Hadoop Data Node UIs at
[http://localhost:9864](http://localhost:9864) and [http://localhost:9865](http://localhost:9865):
all of those URLs should result in a page.

Open up a shell in the master node.
```bash
docker-compose exec master bash
jps
# check service status directly
docker-compose exec master jps
docker-compose exec worker1 jps
docker-compose exec worker2 jps
```
`jps` command outputs a list of running Java processes,

which on Hadoop Namenode/Spark Master node should include those:

<pre>
433 ResourceManager
2209 HMaster
1685 Master
358 SecondaryNameNode
1687 HistoryServer
520 JobHistoryServer
297 NameNode
894 RunJar
2926 Jps
895 RunJar
</pre>
... but not necessarily in this order and those IDs,
also some extras like `RunJar` and `JobHistoryServer` might be there too.

Then let's see if YARN can see all resources we have (2 worker nodes):

```bash
yarn node -list
```
<pre>
current-datetime INFO client.RMProxy: Connecting to ResourceManager at master/172.28.1.1:8032
Total Nodes:2
         Node-Id	     Node-State	Node-Http-Address	Number-of-Running-Containers
   worker1:45019	        RUNNING	     worker1:8042	                           0
   worker2:41001	        RUNNING	     worker2:8042	                           0
</pre>

HDFS (Hadoop distributed file system) condition:
```bash
hdfs dfsadmin -report
```
<pre>
Live datanodes (2):
Name: 172.28.1.2:9866 (worker1)
...
Name: 172.28.1.3:9866 (worker2)
</pre>

Now we'll upload a file into HDFS and see that it's visible from all
nodes:
```bash
hadoop fs -put /data/grades.csv /
hadoop fs -ls /
```
<pre>
Found N items
...
-rw-r--r--   2 root supergroup  ... /grades.csv
...
</pre>
Ctrl+D out of master now. Repeat for remaining nodes
(there's 3 total: master, worker1 and worker2):

```bash
docker-compose exec worker1 bash
hadoop fs -ls /
```
<pre>
Found 1 items
-rw-r--r--   2 root supergroup  ... /grades.csv
</pre>
While we're on nodes other than Hadoop Namenode/Spark Master node,
jps command output should include DataNode and Worker now instead of
NameNode and Master:

```bash
# on worker1 or worker2
docker-compose exec worker2 bash
jps
```
<pre>
1008 HRegionServer
1232 ThriftServer
1633 Jps
237 DataNode
303 NodeManager
</pre>

ThriftServer may only be checked on worker2

### Hive

defaut enginte TEZ

Prerequisite: there's a file `grades.csv` stored in HDFS ( `hadoop fs -put /data/grades.csv /` )
```bash
docker-compose exec master bash
hive
```
```sql
CREATE TABLE grades(
    `Last name` STRING,
    `First name` STRING,
    `SSN` STRING,
    `Test1` DOUBLE,
    `Test2` INT,
    `Test3` DOUBLE,
    `Test4` DOUBLE,
    `Final` DOUBLE,
    `Grade` STRING)
COMMENT 'https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/grades.csv' INTO TABLE grades;

SELECT * FROM grades;
-- OK
-- Alfalfa	Aloysius	123-45-6789	40.0	90	100.0	83.0	49.0	D-
-- Alfred	University	123-12-1234	41.0	97	96.0	97.0	48.0	D+
-- Gerty	Gramma	567-89-0123	41.0	80	60.0	40.0	44.0	C
-- Android	Electric	087-65-4321	42.0	23	36.0	45.0	47.0	B-
-- Bumpkin	Fred	456-78-9012	43.0	78	88.0	77.0	45.0	A-
-- Rubble	Betty	234-56-7890	44.0	90	80.0	90.0	46.0	C-
-- Noshow	Cecil	345-67-8901	45.0	11	-1.0	4.0	43.0	F
-- Buff	Bif	632-79-9939	46.0	20	30.0	40.0	50.0	B+
-- Airpump	Andrew	223-45-6789	49.0	1	90.0	100.0	83.0	A
-- Backus	Jim	143-12-1234	48.0	1	97.0	96.0	97.0	A+
-- Carnivore	Art	565-89-0123	44.0	1	80.0	60.0	40.0	D+
-- Dandy	Jim	087-75-4321	47.0	1	23.0	36.0	45.0	C+
-- Elephant	Ima	456-71-9012	45.0	1	78.0	88.0	77.0	B-
-- Franklin	Benny	234-56-2890	50.0	1	90.0	80.0	90.0	B-
-- George	Boy	345-67-3901	40.0	1	11.0	-1.0	4.0	B
-- Heffalump	Harvey	632-79-9439	30.0	1	20.0	30.0	40.0	C
-- Time taken: 3.324 seconds, Fetched: 16 row(s)
```

Ctrl+D back to bash. Check if the file's been loaded to Hive warehouse
directory:

```bash
hadoop fs -ls /usr/hive/warehouse/grades
```
<pre>
Found 1 items
-rw-r--r--   2 root supergroup  ... /usr/hive/warehouse/grades/grades.csv
</pre>

The table we just created should be accessible from all nodes, let's
verify that now:
```bash
docker-compose exec worker2 bash
hive
```
```sql
SELECT * FROM grades;
```
You should be able to see the same table.
### Spark

sparksql read hive table enabled

Open up [Spark Master Web UI (localhost:8080)](http://localhost:8080/):
<pre>
Workers (2)
Worker Id	Address	State	Cores	Memory
worker-timestamp-172.28.1.3-8882	172.28.1.3:8882	ALIVE	2 (0 Used)	1024.0 MB (0.0 B Used)
worker-timestamp-172.28.1.2-8881	172.28.1.2:8881	ALIVE	2 (0 Used)	1024.0 MB (0.0 B Used)
</pre>
, also worker UIs at  [localhost:8081](http://localhost:8081/)
and  [localhost:8082](http://localhost:8082/). All those pages should be
accessible.

Then there's also Spark History server running at
[localhost:18080](http://localhost:18080/) - every time you run Spark jobs, you
will see them here.

History Server includes REST API at
[localhost:18080/api/v1/applications](http://localhost:18080/api/v1/applications).
This is a mirror of everything on the main page, only in JSON format.

Let's run some sample jobs now:
```bash
docker-compose exec master bash
run-example SparkPi 10
#, or you can do the same via spark-submit:
spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 1g \
    --executor-cores 1 \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    10
```
<pre>
INFO spark.SparkContext: Running Spark version 2.4.4
INFO spark.SparkContext: Submitted application: Spark Pi
..
INFO client.RMProxy: Connecting to ResourceManager at master/172.28.1.1:8032
INFO yarn.Client: Requesting a new application from cluster with 2 NodeManagers
...
INFO yarn.Client: Application report for application_1567375394688_0001 (state: ACCEPTED)
...
INFO yarn.Client: Application report for application_1567375394688_0001 (state: RUNNING)
...
INFO scheduler.DAGScheduler: Job 0 finished: reduce at SparkPi.scala:38, took 1.102882 s
Pi is roughly 3.138915138915139
...
INFO util.ShutdownHookManager: Deleting directory /tmp/spark-81ea2c22-d96e-4d7c-a8d7-9240d8eb22ce
</pre>

Spark has 3 interactive shells: spark-shell to code in Scala,
pyspark for Python and sparkR for R. Let's try them all out:
```bash
hadoop fs -put /data/grades.csv /
spark-shell
```
```scala
spark.range(1000 * 1000 * 1000).count()

val df = spark.read.format("csv").option("header", "true").load("/grades.csv")
df.show()

df.createOrReplaceTempView("df")
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM df WHERE Final > 50").show()

//TODO SELECT TABLE from hive - not working for now.
spark.sql("SELECT * FROM grades").show()
```
<pre>
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = yarn, app id = application_N).
Spark session available as 'spark'.

res0: Long = 1000000000

df: org.apache.spark.sql.DataFrame = [Last name: string, First name: string ... 7 more fields]

+---------+----------+-----------+-----+-----+-----+-----+-----+-----+
|Last name|First name|        SSN|Test1|Test2|Test3|Test4|Final|Grade|
+---------+----------+-----------+-----+-----+-----+-----+-----+-----+
|  Alfalfa|  Aloysius|123-45-6789|   40|   90|  100|   83|   49|   D-|
...
|Heffalump|    Harvey|632-79-9439|   30|    1|   20|   30|   40|    C|
+---------+----------+-----------+-----+-----+-----+-----+-----+-----+

+--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
|        |       df|       true|
+--------+---------+-----------+

+---------+----------+-----------+-----+-----+-----+-----+-----+-----+
|Last name|First name|        SSN|Test1|Test2|Test3|Test4|Final|Grade|
+---------+----------+-----------+-----+-----+-----+-----+-----+-----+
|  Airpump|    Andrew|223-45-6789|   49|    1|   90|  100|   83|    A|
|   Backus|       Jim|143-12-1234|   48|    1|   97|   96|   97|   A+|
| Elephant|       Ima|456-71-9012|   45|    1|   78|   88|   77|   B-|
| Franklin|     Benny|234-56-2890|   50|    1|   90|   80|   90|   B-|
+---------+----------+-----------+-----+-----+-----+-----+-----+-----+
</pre>
Ctrl+D out of Scala shell now.

```bash
pyspark
```
```python
spark.range(1000 * 1000 * 1000).count()

df = spark.read.format('csv').option('header', 'true').load('/grades.csv')
df.show()

df.createOrReplaceTempView('df')
spark.sql('SHOW TABLES').show()
spark.sql('SELECT * FROM df WHERE Final > 50').show()

# TODO SELECT TABLE from hive - not working for now.
spark.sql('SELECT * FROM grades').show()
```
<pre>
1000000000
</pre>

Ctrl+D out of PySpark.

```bash
sparkR
```
```R
df <- as.DataFrame(list("One", "Two", "Three", "Four"), "This is as example")
head(df)

df <- read.df("/grades.csv", "csv", header="true")
head(df)
```
<pre>
  This is as example
1                One
2                Two
3              Three
4               Four
</pre>
* Amazon S3

From Hadoop:
```bash
hadoop fs -Dfs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" -Dfs.s3a.access.key="classified" -Dfs.s3a.secret.key="classified" -ls "s3a://bucket"
```

Then from PySpark:

```python
sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'classified')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'classified')

df = spark.read.format('csv').option('header', 'true').option('sep', '\t').load('s3a://bucket/tabseparated_withheader.tsv')
df.show(5)
```

None of the commands above stores your credentials anywhere
(i.e. as soon as you'd shut down the cluster your creds are safe). More
persistent ways of storing the credentials are out of scope of this
readme.



### Flink

Flink cluster and on yarn mode are both supported

ref: [Flink集群部署详细步骤](https://www.jianshu.com/p/c47e8f438291)

#### Cluster Mode

任务和资源分配有Flink自有集群完成

```shell
# 进入master shell窗口
docker-compose exec master bash
# 启动或者停止集群
$FLINK_HOME/bin/start-cluster.sh
$FLINK_HOME/bin/stop-cluster.sh
# 如下两个命令无法成功运行
 #docker-compose exec master /usr/program/flink/bin/start-cluster.sh
 #docker-compose exec master /usr/program/flink/bin/stop-cluster.sh
```

Check Flink Cluster Status

```shell
# exec jps on master, process blow should show
StandaloneSessionClusterEntrypoint
# exec jps on worker1 or worker2, process blow should show
TaskManagerRunner
```

测试wordcount

```shell
# 必须在start-cluster.sh启动以后执行
rm -rf /data/input.txt
bin/flink run examples/batch/WordCount.jar  --input /data/input.txt --output /data/output.txt
cat /data/output.txt
```

Flink web UI hould be available at http://localhost:45080

#### On YARN Mode

#### 1. 以集群模式提交任务，每次都会新建flink集群

```shell
$FLINK_HOME/bin/flink run -c 入口类名  Jar包 参数 ...
```

运行监控可以在yarn 管理页面查看，而其flink web ui 在yarn 页面最右侧的 ApplicationMaster 链接中

#### 2.启动Session flink集群，提交任务

(1) 启动一个YARN session用2个TaskManager（每个TaskManager分配1GB的堆空间）

```shell
$FLINK_HOME/bin/yarn-session.sh -n 2 -jm 1024 -tm 1024 -s 2
```

(2)  YARN session启动之后就可以使用bin/flink来启动提交作业

```SHELL
# 此时任务会自动通过flink的YARN session 推送至YARN执行
$FLINK_HOME/bin/flink run -c 入口类名  Jar包 参数 ...
```

后台 yarn session

如果你不希望flink yarn client一直运行，也可以启动一个后台运行的yarn session。使用这个参数：-d 或者 --detached

在这种情况下，flink yarn client将会只提交任务到集群然后关闭自己。注意：在这种情况下，无法使用flink停止yarn session。
使用yarn 工具 来停止yarn session

需要在 yarn 管理页面查看<applicationId> 

```bash
yarn application -kill <applicationId> 
```

### Zeppelin

Zeppelin interface should be available at [http://localhost:8890](http://localhost:8890).

You'll find a notebook called "test" in there, containing commands
to test integration with bash, Spark and Livy.

### Livy

Livy is at [http://localhost:8998](http://localhost:8998) (and yes,
there's a web UI as well as REST API on that port - just click the link).

* Livy Sessions.

Try to poll the REST API:
```bash
curl --request GET \
  --url http://localhost:8998/sessions | python3 -mjson.tool
```
The response, assuming you didn't create any sessions before, should look like this:
```json
{
  "from": 0,
  "total": 0,
  "sessions": []
}
```

1 ) Create a session:
```bash
curl --request POST \
  --url http://localhost:8998/sessions \
  --header 'content-type: application/json' \
  --data '{
	"kind": "pyspark"
}' | python3 -mjson.tool
```
Response:
```json
{
    "id": 0,
    "name": null,
    "appId": null,
    "owner": null,
    "proxyUser": null,
    "state": "starting",
    "kind": "pyspark",
    "appInfo": {
        "driverLogUrl": null,
        "sparkUiUrl": null
    },
    "log": [
        "stdout: ",
        "\nstderr: ",
        "\nYARN Diagnostics: "
    ]
}
```

2 ) Wait for session to start (state will transition from "starting"
to "idle"):
```bash
curl --request GET \
  --url http://localhost:8998/sessions/0 | python3 -mjson.tool
```
Response:
```json
{
    "id": 0,
    "name": null,
    "appId": "application_1584274334558_0001",
    "owner": null,
    "proxyUser": null,
    "state": "starting",
    "kind": "pyspark",
    "appInfo": {
        "driverLogUrl": "http://worker2:8042/node/containerlogs/container_1584274334558_0003_01_000001/root",
        "sparkUiUrl": "http://master:8088/proxy/application_1584274334558_0003/"
    },
    "log": [
        "timestamp bla"
    ]
}
```

3 ) Post some statements:
```bash
curl --request POST \
  --url http://localhost:8998/sessions/0/statements \
  --header 'content-type: application/json' \
  --data '{
	"code": "import sys;print(sys.version)"
}' | python3 -mjson.tool
curl --request POST \
  --url http://localhost:8998/sessions/0/statements \
  --header 'content-type: application/json' \
  --data '{
	"code": "spark.range(1000 * 1000 * 1000).count()"
}' | python3 -mjson.tool
```
Response:
```json
{
    "id": 0,
    "code": "import sys;print(sys.version)",
    "state": "waiting",
    "output": null,
    "progress": 0.0,
    "started": 0,
    "completed": 0
}
```
```json
{
    "id": 1,
    "code": "spark.range(1000 * 1000 * 1000).count()",
    "state": "waiting",
    "output": null,
    "progress": 0.0,
    "started": 0,
    "completed": 0
}
```

4) Get the result:
```bash
curl --request GET \
  --url http://localhost:8998/sessions/0/statements | python3 -mjson.tool
```
Response:
```json
{
  "total_statements": 2,
  "statements": [
    {
      "id": 0,
      "code": "import sys;print(sys.version)",
      "state": "available",
      "output": {
        "status": "ok",
        "execution_count": 0,
        "data": {
          "text/plain": "3.7.3 (default, Apr  3 2019, 19:16:38) \n[GCC 8.0.1 20180414 (experimental) [trunk revision 259383]]"
        }
      },
      "progress": 1.0
    },
    {
      "id": 1,
      "code": "spark.range(1000 * 1000 * 1000).count()",
      "state": "available",
      "output": {
        "status": "ok",
        "execution_count": 1,
        "data": {
          "text/plain": "1000000000"
        }
      },
      "progress": 1.0
    }
  ]
}
```

5) Delete the session:
```bash
curl --request DELETE \
  --url http://localhost:8998/sessions/0 | python3 -mjson.tool
```
Response:
```json
{
  "msg": "deleted"
}
```
* Livy Batches.

To get all active batches:
```bash
curl --request GET \
  --url http://localhost:8998/batches | python3 -mjson.tool
```
Strange enough, this elicits the same response as if we were querying
the sessions endpoint, but ok...

1 ) Send the batch:
```bash
curl --request POST \
  --url http://localhost:8998/batches \
  --header 'content-type: application/json' \
  --data '{
	"file": "local:/data/batches/sample_batch.py",
	"pyFiles": [
		"local:/data/batches/sample_batch.py"
	],
	"args": [
		"123"
	]
}' | python3 -mjson.tool
```
Response:
```json
{
    "id": 0,
    "name": null,
    "owner": null,
    "proxyUser": null,
    "state": "starting",
    "appId": null,
    "appInfo": {
        "driverLogUrl": null,
        "sparkUiUrl": null
    },
    "log": [
        "stdout: ",
        "\nstderr: ",
        "\nYARN Diagnostics: "
    ]
}
```

2 ) Query the status:
```bash
curl --request GET \
  --url http://localhost:8998/batches/0 | python3 -mjson.tool
```
Response:
```json
{
    "id": 0,
    "name": null,
    "owner": null,
    "proxyUser": null,
    "state": "running",
    "appId": "application_1584274334558_0005",
    "appInfo": {
        "driverLogUrl": "http://worker2:8042/node/containerlogs/container_1584274334558_0005_01_000001/root",
        "sparkUiUrl": "http://master:8088/proxy/application_1584274334558_0005/"
    },
    "log": [
        "timestamp bla",
        "\nstderr: ",
        "\nYARN Diagnostics: "
    ]
}
```

3 ) To see all log lines, query the `/log` endpoint.
You can skip 'to' and 'from' params, or manipulate them to get all log lines.
Livy (as of 0.7.0) supports no more than 100 log lines per response.
```bash
curl --request GET \
  --url 'http://localhost:8998/batches/0/log?from=100&to=200' | python3 -mjson.tool
```
Response:
```json
{
    "id": 0,
    "from": 100,
    "total": 203,
    "log": [
        "...",
        "Welcome to",
        "      ____              __",
        "     / __/__  ___ _____/ /__",
        "    _\\ \\/ _ \\/ _ `/ __/  '_/",
        "   /__ / .__/\\_,_/_/ /_/\\_\\   version 2.4.5",
        "      /_/",
        "",
        "Using Python version 3.7.5 (default, Oct 17 2019 12:25:15)",
        "SparkSession available as 'spark'.",
        "3.7.5 (default, Oct 17 2019, 12:25:15) ",
        "[GCC 8.3.0]",
        "Arguments: ",
        "['/data/batches/sample_batch.py', '123']",
        "Custom number passed in args: 123",
        "Will raise 123 to the power of 3...",
        "...",
        "123 ^ 3 = 1860867",
        "...",
        "2020-03-15 13:06:09,503 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-138164b7-c5dc-4dc5-be6b-7a49c6bcdff0/pyspark-4d73b7c7-e27c-462f-9e5a-96011790d059"
    ]
}
```

4 ) Delete the batch:
```bash
curl --request DELETE \
  --url http://localhost:8998/batches/0 | python3 -mjson.tool
```
Response:
```json
{
  "msg": "deleted"
}
```



### Sqoop

```shell
#  --schema 一定要放在后面，否则可能导致无运行日志或无法导入数据到指定目录且无法重新执行（报目录已存在）
# PostgreSQL 须设置SET standard_conforming_strings = on;，否则--null-string和--null-non-string不起作用；
#  --null-string和--null-non-string放在-- --schema后面，否则执行时报Can't parse input data: '\N'

# list-databases 用于显示某个连接上所有数据库，也可以直接使用sqoop-list-databases
sqoop list-databases \
--connect jdbc:postgresql://hivemetastore:5432/pgdata --username pguser --password 123456

# list-tables用于显示某个数据库中的所有表：
sqoop list-tables \
--connect jdbc:postgresql://hivemetastore:5432/pgdata --username pguser --password 123456 \
-- --schema "pguser_public" 

# eval 用于执行一个 sql 语句， 并将结果输出到控制台
sqoop eval \
--connect jdbc:postgresql://hivemetastore:5432/pgdata --username pguser --password 123456 \
--query "select * from pguser_public.course"


# 数据从数据库导出到HDFS
# -Dorg.apache.sqoop.splitter.allow_text_splitter=true 针对原表cid的字符串类型报错而添加
sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true 
--connect jdbc:postgresql://hivemetastore:5432/pgdata \
--username pguser --password 123456 \
--table course \
--target-dir /course_export \
--delete-target-dir \
--fields-terminated-by '\t' \
--lines-terminated-by '\n' \
--split-by cid \
-- --schema pguser_public 

# 查看已经导出的文件
hdfs dfs -ls /course_export
hdfs dfs -cat /course_export/*

#  --schema 一定要放在后面，否则可能导致无运行日志或无法导入数据到指定目录且无法重新执行（报目录已存在）
# PostgreSQL 须设置SET standard_conforming_strings = on;，否则--null-string和--null-non-string不起作用；
#  --null-string和--null-non-string放在-- --schema后面，否则执行时报Can't parse input data: '\N'
# 可以增加如下作为空字符串和空的非字符串标识方法
#--null-string含义是 string类型的字段，当Value是NULL，替换成指定的字符，该例子中为@@@
#--null-non-string 含义是非string类型的字段，当Value是NULL，替换成指定字符，该例子中为###

#导入数据库到HIVE表；
# (1)导入已经存在的表。
# 在hive执行：create external table course_hive(cid string,cname string,tid string)row format delimited fields terminated by '\t';
# (2)以不需要提前创建hive表，会自动创建。
sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:postgresql://hivemetastore:5432/pgdata \
--username pguser --password 123456 \
--table course \
--fields-terminated-by '\t' \
--lines-terminated-by '\n' \
--hive-import \
--hive-table course_hive \
--hive-overwrite \
--split-by cid \
-- --schema pguser_public 

# 测试导出结果
hive
select * from course_hive;
# OK
# 01      语文    02
# 02      数学    01
# 03      英语    03
# Time taken: 4.71 seconds, Fetched: 3 row(s)
```



### HBase

Open up [HBase Master Web UI (localhost:16010)](http://localhost:16010/):

```shell
# cd docker compose root dir and access master using bash
docker-compose exec master bash
# hbase shell
hbase shell
# test hbase
status 'summary'
# create table
create 'tbl_user', 'info', 'detail', 'address'
# insert frist data
put 'tbl_user', 'mengday', 'info:id', '1'
put 'tbl_user', 'mengday', 'info:name', '张三'
put 'tbl_user', 'mengday', 'info:age', '28'
# scan table
scan 'tbl_user'
```

ThriftServer is deloyed on worker2. python (etc other APIs) may connect it for service.

### Hudi

hudi jar files should be compiled by yourself, because hudi jars should be builded completely accroding to your hadoop,spark,flink version. please clone [hudi](https://github.com/apache/hudi.git) in github, modify component versions in pom.xml and build it.

https://hudi.apache.org/docs/deployment

Hudi deploys with no long running servers or additional infrastructure cost to your data lake. 

Hudi的部署不需要长期运行的服务器，也不会给数据湖带来额外的基础设施成本。

test case from: https://hudi.apache.org/docs/quick-start-guide ,and some config have been modified.

```scala
// From the extracted directory run spark-shell with Hudi as
// Note: --jars is unnecessary,because those jar file can be discovered by spark
spark-shell \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --jars $SPARK_HOME/jars/hudi-spark3-bundle_2.12-0.10.0.jar,$SPARK_HOME/jars/spark-avro_2.12-3.0.0.jar

// spark-shell
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val tableName = "hudi_trips_cow"
// local(file:// can not be reached,hdfs path should be added)
// WARN: Timeline-server-based markers are not supported for HDFS
val basePath = "hdfs://master:9000/tmp/hudi_trips_cow"
val dataGen = new DataGenerator

// Insert data in spark-shell
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)

// spark-shell
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
// load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

// Update data
// This is similar to inserting new data. Generate updates to existing trips using the data generator, load into a DataFrame and write DataFrame into the hudi table.
// spark-shell
val updates = convertToStringList(dataGen.generateUpdates(10))
val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
// after update,get result again,and manually compare this result with last ones,and difference would show hudi work well 
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
```



### Postgres

open 5432 port for postgre, using JDBC to access the database.

create user `pguser` using password `123456`

create database `pgdata` 

## Credits

Sample data file:
* __grades.csv__ is borrowed from 
[John Burkardt's page](https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html)
under Florida State University domain. Thanks for
sharing those!

* __ssn-address.tsv__ is derived from  __grades.csv__ by removing some fields
  and adding randomly-generated addresses.
  
