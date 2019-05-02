/**
 * 
 */
package org.bawaweb.batch.wordcount.flink;

/**
 * @author Navroz
 * https://riptutorial.com/apache-flink

 *
 */

/*
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import scala.Tuple2;*/

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

public class WordCount {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // input data
        // you can also use env.readTextFile(...) to get words
        DataSet<String> text = env.fromElements(
                "To be, or not to be,--that is the question:--",
                "Is the question?",
                "Whether 'tis nobler in the mind to suffer",
                "Never suffer",
                "The slings and arrows of outrageous fortune",
                "fortun outrageous",
                "Or to take arms against a sea of troubles,",
                "Sea is Troubles"
        );

        /* ===============	non-lambda-style	==============================  
         DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap( new LineSplitter() )
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy( 0 )
                        .aggregate( Aggregations.SUM, 1 );
        
    	====================ends-non-lamba-style	===========================	*/
        DataSet<Tuple2<String, Integer>> counts = text
        	    // split up the lines in pairs (2-tuples) containing: (word,1)
        	    .flatMap( ( String value, Collector<Tuple2<String, Integer>> out ) -> {
        	        // normalize and split the line into words
        	        String[] tokens = value.toLowerCase().split( "\\W+" );

        	        // emit the pairs
        	        for( String token : tokens ){
        	            if( token.length() > 0 ){
        	                out.collect( new Tuple2<>( token, 1 ) );
        	            }
        	        }
        	    } )
                // due to type erasure, we need to specify the return type
                .returns( TupleTypeInfo.getBasicTupleTypeInfo( String.class, Integer.class ) )
        	    // group by the tuple field "0" and sum up tuple field "1"
        	    .groupBy( 0 )
        	    .aggregate( Aggregations.SUM, 1 );

        // emit result
        counts.print();

	}

}