package org.crime;

import org.joda.time.DateTime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;


@Getter
@Setter
@AllArgsConstructor
@DefaultSchema(JavaBeanSchema.class)
public class EnrichedCrime {
    String District;
    String Primary_Type;
    Boolean Arrest;
    Boolean Domestic;
    String FBI_Index_Code;
    DateTime Date;
}
