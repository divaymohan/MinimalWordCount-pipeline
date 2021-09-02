import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;


public class Example {

	public static void main(String[] args) {
		PipelineOptions pipelineOptions = PipelineOptionsFactory.create(); 
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		pipeline.apply(TextIO.read().from("C:\\Users\\dm255078\\OneDrive - Teradata\\Documents\\Learnings\\word-count-beam\\input\\input.txt"))
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {

					@ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split("[^\\p{L}]+")) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply(Count.<String>perElement())
                .apply(ParDo.of(new DoFn<KV<String, Long>, KV<String, Long>>() {
					@ProcessElement
                    public void processElement(ProcessContext c){
						
                        KV<String, Long> element = c.element();
                        if(element.getKey().length() <= 3) {
                            c.output(element);
                        }
                    }
                }))
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String,Long>, String>() {
                    

					@Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() +": " + input.getValue();
                    }
                }))
                .apply(TextIO.write().to("C:\\Users\\dm255078\\OneDrive - Teradata\\Documents\\Learnings\\word-count-beam\\output\\wordcount"));
		
                pipeline.run().waitUntilFinish();
		

	}

}
