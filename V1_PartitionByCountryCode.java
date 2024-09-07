import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

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

    static class FormatOutputFn extends DoFn<KV<String, Iterable<String>>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String countryCode = c.element().getKey();
            for (String line : c.element().getValue()) {
                c.output(line);
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from("gs://your-input-bucket/input.csv"));

        lines.apply("ExtractCountryCode", ParDo.of(new ExtractCountryCodeFn()))
             .apply("GroupByCountryCode", GroupByKey.<String, String>create())
             .apply("FormatOutput", ParDo.of(new FormatOutputFn()))
             .apply("WritePartitions", TextIO.write()
                 .to(new FileBasedSink.FilenamePolicy() {
                     @Override
                     public String unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
                         return String.format("gs://your-output-bucket/output-%s-%05d-of-%05d.csv", 
                                              outputFileHints.getSuggestedFilenameSuffix(), shardNumber, numShards);
                     }
                 })
                 .withNumShards(1)
                 .withSuffix(".csv"));

        pipeline.run();
    }
}
