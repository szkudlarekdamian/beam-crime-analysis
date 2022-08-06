package org.crime;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;


@Getter
@DefaultSchema(JavaBeanSchema.class)
@DefaultCoder(AvroCoder.class)
public class CrimeReported {
  String ID;
  String Case_Number;
@JsonFormat(shape = JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss")
  DateTime Date;
  String Block;
  String IUCR;
  String Primary_Type;
  String Description;
  String Location_Description;
  Boolean Arrest;
  Boolean Domestic;
  String Beat;
  String District;
  String Ward;
  String Community_Area;
  String FBI_Code;
  
  @SchemaCreate
  @JsonCreator
    public CrimeReported(
        @JsonProperty("ID")String ID, 
        @JsonProperty("Case_Number")String Case_Number,
        @JsonProperty("Date") DateTime Date, 
        @JsonProperty("Block")String Block,
        @JsonProperty("IUCR")String IUCR,
        @JsonProperty("Primary_Type")String Primary_Type,
        @JsonProperty("Description")String Description,
        @JsonProperty("Location_Description")String Location_Description,
        @JsonProperty("Arrest")Boolean Arrest,
        @JsonProperty("Domestic")Boolean Domestic,
        @JsonProperty("Beat")String Beat,
        @JsonProperty("District")String District,
        @JsonProperty("Ward")String Ward,
        @JsonProperty("Community_Area")String Community_Area,
        @JsonProperty("FBI_Code")String FBI_Code) {
        this.ID = ID;
        this.Case_Number = Case_Number;
        this.Date = Date;
        this.Block = Block;
        this.IUCR = IUCR;
        this.Primary_Type = Primary_Type;
        this.Description = Description;
        this.Location_Description = Location_Description;
        this.Arrest = Arrest;
        this.Domestic = Domestic;
        this.Beat = Beat;
        this.District = District;
        this.Ward = Ward;
        this.Community_Area = Community_Area;
        this.FBI_Code = FBI_Code;
    }

}
