package org.bawaweb.batch.wordcount.beam;

/**
 * @author Navroz
 *
 */

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BeamBatchWordCount {
	
	public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// get input data
		String inputFilePath = "C:\\Temp\\input1.txt";
		String outputFilePath = "C:\\Temp\\BeamBatchWordCount_output_"+System.currentTimeMillis()+".txt";
	
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeLine = Pipeline.create(options);//Pipeline.create();
		

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