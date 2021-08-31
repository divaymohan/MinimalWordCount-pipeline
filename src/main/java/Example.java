
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;

public class Example {

	public static void main(String[] args) {
		PipelineOptions pipelineOptions = PipelineOptionsFactory.create(); 
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		pipeline.apply(TextIO.read().from("C:\\Users\\dm255078\\OneDrive - Teradata\\Documents\\Learnings\\word-count-beam\\input\\input.txt"))
		.apply("ExtractWords",FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+")) ))
		.apply(Count.<String>perElement())
		.apply("FormatResults",MapElements.into(TypeDescriptors.strings()).via((KV<String,Long> wordCount) -> wordCount.getKey()+": "+wordCount.getValue()))
		.apply(TextIO.write().to("C:\\Users\\dm255078\\OneDrive - Teradata\\Documents\\Learnings\\word-count-beam\\output\\wordcount"));
		
		pipeline.run().waitUntilFinish();
		
	
		

	}

}
