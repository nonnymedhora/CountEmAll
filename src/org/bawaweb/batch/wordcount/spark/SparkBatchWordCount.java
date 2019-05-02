/**
 * 
 */
package org.bawaweb.batch.wordcount.spark;

/**
 * @author Navroz
 *
 */

import java.util.Arrays;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkBatchWordCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String inputFilePath = "C:\\Temp\\input1.txt";
		String outputFilePath = "C:\\Temp\\SparkBatchWordCount_output"+System.currentTimeMillis();

		// To set HADOOP_HOME.
//		System.setProperty("hadoop.home.dir", "c:\\winutils\\");

		// Initialize Spark Context
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf()
					.setAppName("javaSparkWordCount")
					.setMaster("local[4]")
				);

		
		long time = System.currentTimeMillis();
		System.out.println("Started....");
		// Load data from Input File.
		JavaRDD<String> input = sc.textFile(inputFilePath);

		// Split up into words.
		JavaPairRDD<String, Integer> counts = input
												.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
												.mapToPair(word -> new Tuple2<>(word, 1))
												.reduceByKey((a, b) -> a + b);

		System.out.println("counts is " + counts.collect());
		
		counts.saveAsTextFile(outputFilePath);

		sc.stop();
		sc.close();
		
		System.out.println("SparkBatchWordCount time ===> " + (System.currentTimeMillis()-time)/1000 + " seconds");

	}
}


/**
 * 
 * Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
19/02/14 11:59:11 INFO SparkContext: Running Spark version 2.3.0
19/02/14 11:59:12 INFO SparkContext: Submitted application: javaSparkWordCount
19/02/14 11:59:12 INFO SecurityManager: Changing view acls to: Navroz
19/02/14 11:59:12 INFO SecurityManager: Changing modify acls to: Navroz
19/02/14 11:59:12 INFO SecurityManager: Changing view acls groups to: 
19/02/14 11:59:12 INFO SecurityManager: Changing modify acls groups to: 
19/02/14 11:59:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(Navroz); groups with view permissions: Set(); users  with modify permissions: Set(Navroz); groups with modify permissions: Set()
19/02/14 11:59:14 INFO Utils: Successfully started service 'sparkDriver' on port 50203.
19/02/14 11:59:14 INFO SparkEnv: Registering MapOutputTracker
19/02/14 11:59:14 INFO SparkEnv: Registering BlockManagerMaster
19/02/14 11:59:14 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
19/02/14 11:59:14 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
19/02/14 11:59:15 INFO DiskBlockManager: Created local directory at C:\Users\Navroz\AppData\Local\Temp\blockmgr-87170692-eefd-42df-bf94-c9f275dd1fa6
19/02/14 11:59:15 INFO MemoryStore: MemoryStore started with capacity 360.0 MB
19/02/14 11:59:15 INFO SparkEnv: Registering OutputCommitCoordinator
19/02/14 11:59:15 INFO Utils: Successfully started service 'SparkUI' on port 4040.
19/02/14 11:59:16 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.0.2:4040
19/02/14 11:59:16 INFO Executor: Starting executor ID driver on host localhost
19/02/14 11:59:16 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 50222.
19/02/14 11:59:16 INFO NettyBlockTransferService: Server created on 192.168.0.2:50222
19/02/14 11:59:16 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
19/02/14 11:59:16 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.0.2, 50222, None)
19/02/14 11:59:16 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.0.2:50222 with 360.0 MB RAM, BlockManagerId(driver, 192.168.0.2, 50222, None)
19/02/14 11:59:16 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.0.2, 50222, None)
19/02/14 11:59:16 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.0.2, 50222, None)
Started....
19/02/14 11:59:17 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 214.5 KB, free 359.8 MB)
19/02/14 11:59:17 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 20.4 KB, free 359.8 MB)
19/02/14 11:59:17 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.2:50222 (size: 20.4 KB, free: 360.0 MB)
19/02/14 11:59:17 INFO SparkContext: Created broadcast 0 from textFile at SparkBatchWordCount.java:42
19/02/14 11:59:18 INFO FileInputFormat: Total input paths to process : 1
19/02/14 11:59:18 INFO SparkContext: Starting job: collect at SparkBatchWordCount.java:50
19/02/14 11:59:18 INFO DAGScheduler: Registering RDD 3 (mapToPair at SparkBatchWordCount.java:47)
19/02/14 11:59:18 INFO DAGScheduler: Got job 0 (collect at SparkBatchWordCount.java:50) with 2 output partitions
19/02/14 11:59:18 INFO DAGScheduler: Final stage: ResultStage 1 (collect at SparkBatchWordCount.java:50)
19/02/14 11:59:18 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
19/02/14 11:59:18 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 0)
19/02/14 11:59:18 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at mapToPair at SparkBatchWordCount.java:47), which has no missing parents
19/02/14 11:59:18 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 6.0 KB, free 359.8 MB)
19/02/14 11:59:18 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.3 KB, free 359.8 MB)
19/02/14 11:59:18 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.2:50222 (size: 3.3 KB, free: 360.0 MB)
19/02/14 11:59:18 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1039
19/02/14 11:59:18 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at mapToPair at SparkBatchWordCount.java:47) (first 15 tasks are for partitions Vector(0, 1))
19/02/14 11:59:18 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
19/02/14 11:59:18 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 7860 bytes)
19/02/14 11:59:18 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 7860 bytes)
19/02/14 11:59:18 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
19/02/14 11:59:18 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
19/02/14 11:59:18 INFO HadoopRDD: Input split: file:/C:/Temp/input1.txt:0+74
19/02/14 11:59:18 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1153 bytes result sent to driver
19/02/14 11:59:18 INFO HadoopRDD: Input split: file:/C:/Temp/input1.txt:74+74
19/02/14 11:59:18 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 434 ms on localhost (executor driver) (1/2)
19/02/14 11:59:19 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1110 bytes result sent to driver
19/02/14 11:59:19 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 449 ms on localhost (executor driver) (2/2)
19/02/14 11:59:19 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
19/02/14 11:59:19 INFO DAGScheduler: ShuffleMapStage 0 (mapToPair at SparkBatchWordCount.java:47) finished in 0.792 s
19/02/14 11:59:19 INFO DAGScheduler: looking for newly runnable stages
19/02/14 11:59:19 INFO DAGScheduler: running: Set()
19/02/14 11:59:19 INFO DAGScheduler: waiting: Set(ResultStage 1)
19/02/14 11:59:19 INFO DAGScheduler: failed: Set()
19/02/14 11:59:19 INFO DAGScheduler: Submitting ResultStage 1 (ShuffledRDD[4] at reduceByKey at SparkBatchWordCount.java:48), which has no missing parents
19/02/14 11:59:19 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.7 KB, free 359.8 MB)
19/02/14 11:59:19 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.1 KB, free 359.8 MB)
19/02/14 11:59:19 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.2:50222 (size: 2.1 KB, free: 360.0 MB)
19/02/14 11:59:19 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1039
19/02/14 11:59:19 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (ShuffledRDD[4] at reduceByKey at SparkBatchWordCount.java:48) (first 15 tasks are for partitions Vector(0, 1))
19/02/14 11:59:19 INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
19/02/14 11:59:19 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, ANY, 7649 bytes)
19/02/14 11:59:19 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, ANY, 7649 bytes)
19/02/14 11:59:19 INFO Executor: Running task 1.0 in stage 1.0 (TID 3)
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 15 ms
19/02/14 11:59:19 INFO Executor: Finished task 1.0 in stage 1.0 (TID 3). 1429 bytes result sent to driver
19/02/14 11:59:19 INFO Executor: Running task 0.0 in stage 1.0 (TID 2)
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
19/02/14 11:59:19 INFO Executor: Finished task 0.0 in stage 1.0 (TID 2). 1323 bytes result sent to driver
19/02/14 11:59:19 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 124 ms on localhost (executor driver) (1/2)
19/02/14 11:59:19 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 156 ms on localhost (executor driver) (2/2)
19/02/14 11:59:19 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
19/02/14 11:59:19 INFO DAGScheduler: ResultStage 1 (collect at SparkBatchWordCount.java:50) finished in 0.234 s
counts is [(talk.,1), (are,2), (only,1), (as,8), (,1), (they,7), (love,,1), (not,1), (people,1), (share.,1), (or,1), (care,1), (beautiful,2), (walk,1), (look,,1)]
19/02/14 11:59:19 INFO DAGScheduler: Job 0 finished: collect at SparkBatchWordCount.java:50, took 1.210970 s
19/02/14 11:59:19 INFO deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
19/02/14 11:59:19 INFO SparkContext: Starting job: runJob at SparkHadoopWriter.scala:78
19/02/14 11:59:19 INFO DAGScheduler: Got job 1 (runJob at SparkHadoopWriter.scala:78) with 2 output partitions
19/02/14 11:59:19 INFO DAGScheduler: Final stage: ResultStage 3 (runJob at SparkHadoopWriter.scala:78)
19/02/14 11:59:19 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
19/02/14 11:59:19 INFO DAGScheduler: Missing parents: List()
19/02/14 11:59:19 INFO DAGScheduler: Submitting ResultStage 3 (MapPartitionsRDD[5] at saveAsTextFile at SparkBatchWordCount.java:52), which has no missing parents
19/02/14 11:59:19 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 66.2 KB, free 359.7 MB)
19/02/14 11:59:19 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 23.9 KB, free 359.7 MB)
19/02/14 11:59:19 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.2:50222 (size: 23.9 KB, free: 360.0 MB)
19/02/14 11:59:19 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1039
19/02/14 11:59:19 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (MapPartitionsRDD[5] at saveAsTextFile at SparkBatchWordCount.java:52) (first 15 tasks are for partitions Vector(0, 1))
19/02/14 11:59:19 INFO TaskSchedulerImpl: Adding task set 3.0 with 2 tasks
19/02/14 11:59:19 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 4, localhost, executor driver, partition 0, ANY, 7649 bytes)
19/02/14 11:59:19 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 5, localhost, executor driver, partition 1, ANY, 7649 bytes)
19/02/14 11:59:19 INFO Executor: Running task 0.0 in stage 3.0 (TID 4)
19/02/14 11:59:19 INFO Executor: Running task 1.0 in stage 3.0 (TID 5)
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
19/02/14 11:59:19 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
19/02/14 11:59:19 INFO FileOutputCommitter: Saved output of task 'attempt_20190214115919_0005_m_000001_0' to file:/C:/Temp/SparkBatchWordCount_output1550125750713/_temporary/0/task_20190214115919_0005_m_000001
19/02/14 11:59:19 INFO SparkHadoopMapRedUtil: attempt_20190214115919_0005_m_000001_0: Committed
19/02/14 11:59:19 INFO Executor: Finished task 1.0 in stage 3.0 (TID 5). 1416 bytes result sent to driver
19/02/14 11:59:19 INFO TaskSetManager: Finished task 1.0 in stage 3.0 (TID 5) in 313 ms on localhost (executor driver) (1/2)
19/02/14 11:59:19 INFO FileOutputCommitter: Saved output of task 'attempt_20190214115919_0005_m_000000_0' to file:/C:/Temp/SparkBatchWordCount_output1550125750713/_temporary/0/task_20190214115919_0005_m_000000
19/02/14 11:59:19 INFO SparkHadoopMapRedUtil: attempt_20190214115919_0005_m_000000_0: Committed
19/02/14 11:59:19 INFO Executor: Finished task 0.0 in stage 3.0 (TID 4). 1459 bytes result sent to driver
19/02/14 11:59:19 INFO TaskSetManager: Finished task 0.0 in stage 3.0 (TID 4) in 344 ms on localhost (executor driver) (2/2)
19/02/14 11:59:19 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
19/02/14 11:59:19 INFO DAGScheduler: ResultStage 3 (runJob at SparkHadoopWriter.scala:78) finished in 0.391 s
19/02/14 11:59:19 INFO DAGScheduler: Job 1 finished: runJob at SparkHadoopWriter.scala:78, took 0.423742 s
19/02/14 11:59:19 INFO SparkHadoopWriter: Job job_20190214115919_0005 committed.
19/02/14 11:59:19 INFO SparkUI: Stopped Spark web UI at http://192.168.0.2:4040
19/02/14 11:59:19 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/02/14 11:59:20 INFO MemoryStore: MemoryStore cleared
19/02/14 11:59:20 INFO BlockManager: BlockManager stopped
19/02/14 11:59:20 INFO BlockManagerMaster: BlockManagerMaster stopped
19/02/14 11:59:20 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
SparkBatchWordCount time ===> 3 seconds
19/02/14 11:59:20 INFO SparkContext: Successfully stopped SparkContext
19/02/14 11:59:20 INFO SparkContext: SparkContext already stopped.
19/02/14 11:59:20 INFO ShutdownHookManager: Shutdown hook called
19/02/14 11:59:20 INFO ShutdownHookManager: Deleting directory C:\Users\Navroz\AppData\Local\Temp\spark-b50499b1-ad07-4bb0-9c2f-4780524831a5

 */

