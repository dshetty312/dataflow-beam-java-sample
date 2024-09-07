import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class PartitionByCountryCode {

    static class ExtractCountryCodeFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] parts = c.element().split(",");
            if (parts.length == 2) {
                String name = parts[0];
                String countryCode = parts[1];
                c.output(KV.of(countryCode, c.element()));
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from("gs://your-input-bucket/input.csv"));

        PCollection<KV<String, String>> kvPairs = lines.apply("ExtractCountryCode", ParDo.of(new ExtractCountryCodeFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

        kvPairs.apply("WritePartitions", FileIO.<String, KV<String, String>>writeDynamic()
                .by((SerializableFunction<KV<String, String>, String>) KV::getKey)
                .via(Contextful.fn(KV::getValue), TextIO.sink())
                .to("gs://your-output-bucket/")
                .withNaming(key -> FileIO.Write.defaultNaming("output-" + key, ".csv"))
                .withNumShards(1));

        pipeline.run();
    }
}
