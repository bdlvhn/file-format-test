package org.donghoon.parquet;

import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.donghoon.entity.MapData;
import org.donghoon.util.LocalInputFile;

import java.io.*;
import java.time.Instant;
import java.util.*;

public class ParquetFileReader {
    File file;

    public ParquetFileReader(String path) {
        this.file = new File(path);
    }

    public List<MapData> read() {


        List<MapData> records = new ArrayList<>();
        try (ParquetReader<Object> parquetReader = AvroParquetReader.builder(new LocalInputFile(file.toPath()))
                .withConf(new Configuration())
                .build()) {
            GenericData.Record value;
            int numRecords = getNumRecords();

            for (int i=0; i<numRecords; i++) {
                value = (GenericData.Record) parquetReader.read();
                if (value == null) {
                    System.out.printf("Retrieved %d records%n", records.size());
                    return records;
                } else {
                    GenericRecord genericRecord = deserialize(value.getSchema(), toByteArray(value.getSchema(), value));
                    MapData record = MapData.builder()
                            .logtime(Date.from((Instant) genericRecord.get("logtime")))
                            .timezone(genericRecord.get("timezone").toString())
                            .logid(Integer.parseInt(genericRecord.get("logid").toString()))
                            .seq(Integer.parseInt(genericRecord.get("seq").toString()))
                            .info(genericRecord.get("info").toString())
                            .plogdate(genericRecord.get("plogdate").toString())
                            .build();
                    records.add(record);
                }
            }
            return records;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public int getNumRecords() throws IOException {
        try (ParquetReader<Object> parquetReader =
                     AvroParquetReader.builder(new LocalInputFile(file.toPath()))
                             .withDataModel(GenericData.get())
                             .withConf(new Configuration())
                             .build()) {
            GenericData.Record value;
            int i = 0;
            while (true) {
                value = (GenericData.Record) parquetReader.read();
                if (value == null) {
                    return i;
                } else {
                    i++;
                }
            }
        }
    }

    private GenericRecord deserialize(Schema schema, byte[] data) throws IOException {
        GenericData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        InputStream is = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, GenericData.get());
        return reader.read(null, decoder);
    }

    private byte[] toByteArray(Schema schema, GenericRecord genericRecord) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.getData().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(genericRecord, encoder);
        encoder.flush();
        return baos.toByteArray();
    }

}
