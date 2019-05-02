/**
 * 
 */
package org.bawaweb.batch.wordcount.beam.runners.flink;

import java.util.Arrays;

//import org.apache.beam.runners.flink.FlinkPipelineExecutionEnvironment;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * @author Navroz
 *
 */
public class BeamFlinkRunnerWordCount {
	
	public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// get input data
		String inputFilePath = "C:\\Temp\\input1.txt";
		String outputFilePath = "C:\\Temp\\BeamFlinkRunnerWordCount_output_"+System.currentTimeMillis()+".txt";
	
		PipelineOptions options = PipelineOptionsFactory.create();
		options.as(FlinkPipelineOptions.class)
	          .setRunner(FlinkRunner.class);
		
		Pipeline pipeLine = Pipeline.create(options);//Pipeline.create();
		
//		FlinkPipelineExecutionEnvironment env = new FlinkPipelineExecutionEnvironment(options);
		

		pipeLine
	
			.apply(TextIO.read().from(inputFilePath))
			
			.apply(
		            FlatMapElements.into(TypeDescriptors.strings())
		            .via((String word) -> Arrays.asList(word.split(TOKENIZER_PATTERN))))
			
			.apply(Filter.by((String word) -> !word.isEmpty()))
			
			.apply(Count.perElement())
			
			 .apply(
			            MapElements.into(TypeDescriptors.strings())
			                .via(
			                    (KV<String, Long> wordCount) ->
			                        wordCount.getKey() + ": " + wordCount.getValue()))
			 
	
		     .apply(TextIO.write().to(outputFilePath).withoutSharding());	//withoutSharding	=	1 file created
		
		pipeLine.run().waitUntilFinish();

	}

}
