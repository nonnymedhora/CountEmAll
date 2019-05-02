/**
 * 
 */
package org.bawaweb.streaming.wordcount.flink;

/**
 * @author Navroz
 *
 */

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkStreamingWordCount {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		// get input data
		String inputFilePath = "C:\\Temp\\input1.txt";
		String outputFilePath = "C:\\Temp\\FlinkStreamingWordCount_output"+System.currentTimeMillis();
		
		// set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		
		long time = System.currentTimeMillis();
		System.out.println("Started....");
    
        // get input data
        /*DataStreamSource<String> source = env.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles"
        );*/
        DataStreamSource<String> source = env.readTextFile(inputFilePath);
        
        SingleOutputStreamOperator<Tuple2<String, Integer>> results = source
                // split up the lines in pairs (2-tuples) containing: (word,1)
                .flatMap( ( String value, Collector<Tuple2<String, Integer>> out ) -> {
                    // emit the pairs
                    for( String token : value.toLowerCase().split( "\\W+" ) ){
                        if( token.length() > 0 ){
                            out.collect( new Tuple2<>( token, 1 ) );
                        }
                    }
                } )
                // due to type erasure, we need to specify the return type
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, Integer.class ) )
                // group by the tuple field "0"
                .keyBy( 0 )
                // sum up tuple on field "1"
                .sum( 1 );
        
        
//                // print the result
//                .print();
        
        results.print();
        
        results.writeAsText(outputFilePath);

        // start the job
        env.execute("FlinkStreamingWordCount");
        
        System.out.println("FlinkStreaminghWordCount time ===> " + (System.currentTimeMillis()-time)/1000 + " seconds");

	}

}

/***
 * Started....
log4j:WARN No appenders could be found for logger (org.apache.flink.api.java.typeutils.TypeExtractor).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
2> (they,1)
1> (are,1)
2> (as,1)
1> (only,1)
1> (care,1)
1> (share,1)
2> (beautiful,1)
2> (as,2)
2> (they,2)
2> (love,1)
2> (as,3)
2> (they,3)
2> (as,4)
2> (they,4)
1> (people,1)
1> (are,2)
1> (not,1)
1> (look,1)
1> (talk,1)
2> (as,5)
2> (beautiful,2)
2> (as,6)
2> (they,5)
2> (as,7)
2> (they,6)
2> (walk,1)
2> (or,1)
2> (as,8)
2> (they,7)
FlinkStreaminghWordCount time ===> 12 seconds
 */



