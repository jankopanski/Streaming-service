import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import java.io.IOException;

import static java.lang.Math.abs;

public class Collector {
    private static final int DURATION = 30;
    private static final Integer HASH_OFFSET = 0;
    private static final String output_file = "collector_output.txt";

    public static void main(String[] args) throws IOException {
        String[] args1 =new String[]{ "--hdfsConfiguration=[{\"fs.defaultFS\" : \"hdfs://localhost:9000\"}]"};
        HadoopFileSystemOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(args1)
                .withValidation()
                .as(HadoopFileSystemOptions.class);

        FileSystems.setDefaultPipelineOptions(pipelineOptions);

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply(kafkaReader())
                .apply(ParDo.of(new CollectFn()))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(DURATION))))
                .apply(TextIO.write().to("hdfs://localhost:9000/" + output_file).withWindowedWrites().withoutSharding());

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    private static PTransform<PBegin, PCollection<KV<String, String>>> kafkaReader() {
        return KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("stream")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata();
    }

    static class CollectFn extends DoFn<KV<String, String>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, String> entry, OutputReceiver<String> out) {
            String value = entry.getValue();
            final String regex = "^\\((\\d+),(\\d+),(\\d+)\\)$";
            if (value.matches(regex)) {
                String[] splitsStr = value.substring(1, value.length() - 1).split(",");
                Integer clip = Integer.parseInt(splitsStr[1]);
                int rating = Integer.parseInt(splitsStr[2]);
                if (rating >= 1 && rating <= 10) {
                    Integer toHashValue = clip + HASH_OFFSET;
                    int clipHash = abs(toHashValue.hashCode()) % 100;
                    if (clipHash < 3) {
                        out.output(splitsStr[0] + ";" + splitsStr[1] + ";" + splitsStr[2]);
                    }
                }
            }
        }
    }
}
