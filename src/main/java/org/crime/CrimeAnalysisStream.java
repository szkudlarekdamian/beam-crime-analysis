package org.crime;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.beam.runners.samza.SamzaRunner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.schemas.transforms.Join.FieldsEqual;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.crime.CrimeCombine.CrimeFinal;
import org.crime.CrimeCombine.CrimeTotals;


public class CrimeAnalysisStream {


      static class ExtractTimestamps 
            extends DoFn<EnrichedCrime, EnrichedCrime> {  
        @Override
        public Duration getAllowedTimestampSkew() {
            return new Duration(Long.MAX_VALUE);
        }
        @ProcessElement
        public void processElement(
                @Element EnrichedCrime element,
                OutputReceiver<EnrichedCrime> out) {
            // Extract the timestamp from entry we're currently processing.
            Instant crimeTimeStamp = element.getDate().toInstant();
            // Use OutputReceiver.outputWithTimestamp (rather than
            // OutputReceiver.output) to emit the entry with timestamp attached.
            out.outputWithTimestamp(element, crimeTimeStamp);
        }
      }

    static void runStreamingQuery(PipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        PCollection<CrimeReported> crimes = p
            .apply("ReadCrimesFromKafka",
                KafkaIO.<String, CrimeReported>read()
                    .withBootstrapServers("localhost:9092")
                    .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
                    .withTopic("crimes")
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(CrimeReportedDeserializer.class )
                    .withoutMetadata())
            .apply("GetMessageValue",
                Values.create());
        
        final PCollectionView<Map<String, String>> iucrsView = p
            .apply("ReadIUCR", 
                TextIO.read().from("samza-beam-examples/data/IUCR_codes.csv"))
            .apply("ParseIUCR",
                ParDo.of(new DoFn<String, KV<String,String>>() {
                    @ProcessElement
                    public void processElement(
                            @Element String element,
                            OutputReceiver<KV<String,String>> receiver) {
                        String[] fields = element.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        receiver.output(KV.of(fields[0], fields[3]));
                    }
                  }
                ))
            .apply("ConvertIUCRToMap",
                View.asMap());
                

       
        PCollection<EnrichedCrime> enrichedCrimes = 
            crimes.apply("EnrichCrimes",
                ParDo.of(new DoFn<CrimeReported, EnrichedCrime>() {
                @ProcessElement
                public void processElement(ProcessContext processContext) {
                    Map<String, String> iucrs= processContext.sideInput(iucrsView);
                    CrimeReported element = processContext.element();
                    String fbi_index_code = iucrs.getOrDefault(element.getIUCR(), "N");

                    EnrichedCrime enrichedCrime = new EnrichedCrime(
                        element.getDistrict(),
                        element.getPrimary_Type(),
                        element.getArrest(),
                        element.getDomestic(),
                        fbi_index_code,
                        element.getDate());
                    processContext.output(enrichedCrime);
                }}).withSideInputs(iucrsView));
   

        PCollection<EnrichedCrime> windowedCrimes = enrichedCrimes
            .apply("ExtractTimestamps",
                ParDo.of(new ExtractTimestamps()))
            .apply("ApplySlidingWindow",
                Window.<EnrichedCrime>into(
                    SlidingWindows
                        .of(Duration.standardDays(7))
                        .every(Duration.standardDays(1)))
                .withAllowedLateness(Duration.standardDays(365))
                .accumulatingFiredPanes()
                );

        PCollection<KV<String,CrimeFinal>> districtStats = windowedCrimes
            .apply("DistrictAsKey",
                WithKeys.of(EnrichedCrime::getDistrict).withKeyType(TypeDescriptors.strings()))
            .apply("PerformCrimeCombine",
                Combine.<String, EnrichedCrime, CrimeFinal>perKey(new CrimeCombine()));

        districtStats
            .apply("FormatOutput",
                ParDo.of(new DoFn<KV<String,CrimeFinal>, String>(){
                    @ProcessElement
                    public void processElement(@Element KV<String,CrimeFinal> element, ProcessContext processContext, IntervalWindow window) {  
                            processContext.output(
                                "[" + window.start().toString() +" : "+window.end().toString() + "] "
                                + element.getKey() +" : "+element.getValue().toString()
                            );
                    }
                }))
            .apply("FinalWriteToFile",
                TextIO.write().withWindowedWrites().to("samza-beam-examples/data/report.txt")
            .withNumShards(0));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("crime-stream-analysis");
        options.setRunner(SamzaRunner.class);
    
        runStreamingQuery(options);
      }
}
