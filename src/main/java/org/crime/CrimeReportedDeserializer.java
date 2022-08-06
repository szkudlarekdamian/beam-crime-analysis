package org.crime;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

public class CrimeReportedDeserializer implements Deserializer<CrimeReported> {
    @Override
    public void close() {
    }

    @Override
    public CrimeReported deserialize(String arg0, byte[] arg1) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        JsonMapper mapper = new JsonMapper();
        mapper.registerModule(new JodaModule());
        mapper.setDateFormat(df);
        CrimeReported crime = null;
        try {
            crime = mapper.readValue(arg1, CrimeReported.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return crime;
    }

    @Override
    public void configure(Map configs, boolean isKey) {        
    }

}
