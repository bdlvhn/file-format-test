package org.donghoon.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.donghoon.util.Constants.logids;
import static org.donghoon.util.Constants.timezones;

public class Generator {

    Integer seq = 0;

    public List<GenericData.Record> generateRecords(Schema schema, int n) {
        List<GenericData.Record> recordList = new ArrayList<>();

        for (int i=0; i<n; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("logtime", generateLogtime());
            record.put("timezone", generateRandFromListString(timezones));
            record.put("logid",  generateRandFromListInteger(logids));
            record.put("seq", generateSeq());
            record.put("info", generateString(100));
            record.put("plogdate","20220901");

            recordList.add(record);
        }

        return recordList;
    }

    public List<Map<String, Object>> generateHashMap(int n) {
        List<Map<String, Object>> data = new LinkedList<>();
        for (int i=0; i<n; i++) {
            HashMap<String, Object> row = new HashMap<String, Object>() {{
                put("logtime", generateLogtime());
                put("timezone", generateRandFromListString(timezones));
                put("logid", generateRandFromListInteger(logids));
                put("seq", generateSeq());
                put("info", generateString(100));
                put("plogdate", "20220901");
            }};
            data.add(row);
        }
        return data;
    }

    public List<JSONObject> generateJson(int n) {
        List<JSONObject> data = new ArrayList<>();
        for (int i=0; i<n; i++) {
            HashMap<String, Object> row = new HashMap<String, Object>() {{
                put("logtime", generateLogtime());
                put("timezone", generateRandFromListString(timezones));
                put("logid", generateRandFromListInteger(logids));
                put("seq", generateSeq());
                put("info", generateString(100));
                put("plogdate", "20220901");
            }};
            data.add(new JSONObject(row));
        }
        return data;
    }

    public long generateLogtime() {
        Instant start = Instant.now().minus(Duration.ofDays(30));
        Instant end = Instant.now();
        long random = ThreadLocalRandom.current().nextLong(start.getEpochSecond(), end.getEpochSecond());

        return random;
    }

    public String generateRandFromListString(List<String> array) {
        Random rand = new Random();
        return array.get(rand.nextInt(array.size()));
    }

    public Integer generateRandFromListInteger(List<Integer> array) {
        Random rand = new Random();
        return array.get(rand.nextInt(array.size()));
    }

    public Integer generateSeq() {
        seq++;
        return seq;
    }

    public String generateString(int length) {
        int leftLimit = 48;
        int rightLimit = 122;
        int targetStringLength = length;
        Random random = new Random();

        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder:: append)
                .toString();

        return generatedString;
    }
}
