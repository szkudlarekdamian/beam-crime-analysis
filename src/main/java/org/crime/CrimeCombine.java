package org.crime;

import java.io.Serializable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

public class CrimeCombine extends CombineFn<EnrichedCrime, CrimeCombine.CrimeTotals, CrimeCombine.CrimeFinal> {
    public static class CrimeTotals implements Serializable{
        Long totalCrimes = 0L;
        Long totalArrests = 0L;
        Long totalDomestic = 0L;
        Long totalFBI = 0L;
        Long totalNarcotics = 0L;
    }
    
    @ToString
    @DefaultCoder(AvroCoder.class)
    @AllArgsConstructor
    public static class CrimeFinal{
        Long totalCrimes = 0L;
        Double percentageArrests = 0.0;
        Double percentageDomestic = 0.0;
        Double percentageFBI = 0.0;
        Double percentageNarcotics = 0.0;
        
    }
  
    public Double percentage(Double number){

        return Math.round(number * 10000D) / 100D ;
    }

    @Override
    public CrimeTotals createAccumulator() { return new CrimeTotals(); }
  
    @Override
    public CrimeTotals addInput(CrimeTotals accum, EnrichedCrime crime) {
        accum.totalCrimes++;
        if (crime.Arrest) {
            accum.totalArrests++;
        }
        if (crime.Domestic) {
            accum.totalDomestic++;
        }
        if (crime.FBI_Index_Code.equals("I")) {
            accum.totalFBI++;
        }
        if (crime.Primary_Type.toUpperCase().equals("NARCOTICS")) {
            accum.totalNarcotics++;
        }
        return accum;
    }
  
    @Override
    public CrimeTotals mergeAccumulators(Iterable<CrimeTotals> accums) {
        CrimeTotals merged = createAccumulator();
        for (CrimeTotals accum : accums) {
            merged.totalCrimes += accum.totalCrimes;
            merged.totalArrests += accum.totalArrests;
            merged.totalDomestic += accum.totalDomestic;
            merged.totalFBI += accum.totalFBI;
            merged.totalNarcotics += accum.totalNarcotics;
        }
        return merged;
    }
  
    @Override
    public CrimeFinal extractOutput(CrimeTotals accum) {
      Double total = accum.totalCrimes.doubleValue();
      return new CrimeFinal(
            accum.totalCrimes,
            percentage(accum.totalArrests/total),
            percentage(accum.totalDomestic/total),
            percentage(accum.totalFBI/total),
            percentage(accum.totalNarcotics/total)
        );
    }
  }