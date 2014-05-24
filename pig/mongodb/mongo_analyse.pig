REGISTER mongo-hadoop-core_2.2.0-1.2.0.jar
REGISTER mongo-hadoop-pig_2.2.0-1.2.0.jar
REGISTER mongo-java-driver-2.11.1.jar
set default_parallel 1

DEFINE MongoLoader com.mongodb.hadoop.pig.MongoLoader;

A = LOAD 'mongodb://localhost:27017/test.user' USING MongoLoader() AS (user: map[]);
B = FOREACH A GENERATE user#'id', user#'name', user#'email';
DUMP B;
