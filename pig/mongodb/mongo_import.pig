REGISTER /tmp/udfs/mongo-hadoop-core_2.2.0-1.2.0.jar
REGISTER /tmp/udfs/mongo-hadoop-pig_2.2.0-1.2.0.jar
REGISTER /tmp/udfs/mongo-java-driver-2.11.1.jar
set default_parallel 1

/* disable speculative execution */
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

DEFINE MongoInsertStorage com.mongodb.hadoop.pig.MongoInsertStorage;

/* load and group data */
A = LOAD '$INPUT' USING PigStorage() AS (userId:int, memberId:int, time:long);
B = GROUP A BY memberId;
C = FOREACH B GENERATE group AS memberId, $1 AS items;
/* write to mongo */
STORE C INTO 'mongodb://localhost:27017/test.useritem' USING MongoInsertStorage('items:bag', 'memberId');
