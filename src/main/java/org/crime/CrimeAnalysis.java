package org.crime;


import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTime;

public class CrimeAnalysis {

  static class ParseLines extends DoFn<String, CrimeReported> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<CrimeReported> receiver) {
      // Split the line into words.
      String[] fields = element.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

      try{
        DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a");
        DateTime dateTime = DateTime.parse(fields[2], formatter);
        CrimeReported crime = new CrimeReported(
          fields[0], fields[1], dateTime, fields[3],
          fields[4], fields[5], fields[6], fields[7],
          new Boolean(fields[8]), new Boolean(fields[9]),
          fields[10], fields[11], fields[12], fields[13],
          fields[14]);
        receiver.output(crime);
      }catch (Exception e) {}
    }
  }
    
 

    static void runBatchQueries(PipelineOptions options) {
        Pipeline p = Pipeline.create(options);
    
        PCollection<CrimeReported> crimes = p
          .apply("ReadLines",
            TextIO.read().from("samza-beam-examples/data/Crimes2022.csv"))
          .apply("ParseCrimeReported",
            ParDo.of(new ParseLines()));
            
        PCollection<String> crimesByHour = crimes
          .apply("ConvertCrimeReportedToRow",
            Convert.toRows())
          .apply("SQLQuery-GroupHours",
            SqlTransform.query(
             " SELECT HOUR(`date`) as `Hour`, COUNT(*) as `Count`"
            +" FROM PCOLLECTION"
            +" GROUP BY HOUR(`date`)"
            +" ORDER BY `Count` DESC"
            +" LIMIT 24"))
          .apply("FormatOutput",
            MapElements
              .into(TypeDescriptors.strings())
              .via((Row row) -> row.toString()));
        // crimesByHour.apply("WriteCounts", TextIO.write().to("samza-beam-examples/data/hours.txt").withoutSharding());

        
        PCollection<String> crimesBySchool = crimes
          .apply("FilterSchoolLocations",
            Filter.by(new SerializableFunction<CrimeReported, Boolean>() {
              @Override
              public Boolean apply(CrimeReported c) {
                return c.getLocation_Description().contains("SCHOOL");
              }
            }))
          .apply("ApplyKeys-SchoolType",
            WithKeys.of(new SerializableFunction<CrimeReported, String>() {
              @Override
              public String apply(CrimeReported c) {
                return (c.getLocation_Description().contains("PRIVATE")) ? "PRIVATE" : "PUBLIC";
              }
            }))
          .apply("CountSchoolTypes",
            Count.perKey())
          .apply("FormatOutput",
            MapElements
            .into(TypeDescriptors.strings())
            .via(
                (KV<String, Long> kv) -> kv.getKey() + ": " + kv.getValue()
            ));

        p.run().waitUntilFinish();
      }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("crime-analysis");
        options.setRunner(SamzaRunner.class);
    
        runBatchQueries(options);
      }
    }