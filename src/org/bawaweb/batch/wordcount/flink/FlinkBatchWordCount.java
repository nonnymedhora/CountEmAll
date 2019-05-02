
package org.bawaweb.batch.wordcount.flink;

/**
 * @author Navroz
 * https://data-flair.training/blogs/apache-flink-application/

 *
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class FlinkBatchWordCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// set up the execution environment
			final ParameterTool params = ParameterTool.fromArgs(args);
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.getConfig().setGlobalJobParameters(params);
			
			// get input data
			String inputFilePath = "C:\\Temp\\input1.txt";
			String outputFilePath = "C:\\Temp\\FlinkBatchWordCount"+System.currentTimeMillis();
			
			long time = System.currentTimeMillis();
			System.out.println("Started....");
			
			DataSet<String> text = env.readTextFile(inputFilePath);//(params.get("input"));
			DataSet<Tuple2<String, Integer>> counts =
					// split up the lines in pairs (2-tuples) containing: (word,1)
					text.flatMap(new Splitter())
							// group by the tuple field "0" and sum up tuple field "1"
							.groupBy(0).aggregate(Aggregations.SUM, 1);
			
			counts.print();
			// emit result
			counts.writeAsText(outputFilePath);//(params.get("output"));
			// execute program
			env.execute("WordCount Example");
			
			System.out.println("FlinkBatchWordCount time ===> " + (System.currentTimeMillis()-time)/1000 + " seconds");
		}
}


	
/********
 * 
 * Started....
log4j:WARN No appenders could be found for logger (org.apache.flink.api.java.ExecutionEnvironment).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
(beautiful,2)
(look,1)
(love,1)
(not,1)
(share,1)
(talk,1)
(walk,1)
(are,2)
(as,8)
(care,1)
(only,1)
(or,1)
(people,1)
(they,7)
FlinkBatchWordCount time ===> 12 seconds	[prvsrun-was-10,9]
 ********************/


		// The operations are defined by specialized classes, here the Splitter class.
		class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

			private static final long serialVersionUID = 18765L;

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
				// normalize and split the line into words
				String[] tokens = value.split("\\W+");
				// emit the pairs
				for (String token : tokens) {
					if (token.length() > 0) {
						out.collect(new Tuple2<String, Integer>(token, 1));
					}
				}
			}
		}


		