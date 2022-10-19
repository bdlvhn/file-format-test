package org.donghoon.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.donghoon.util.Generator;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.donghoon.util.Constants.*;

public class ParquetFileWriter {

    public static void write(Generator generator) {
        try {
            Path path = new Path(outputPath + "/" + "map_data" + ".parquet");
            Files.deleteIfExists(Paths.get(path.toString()));

            Schema schema = new Schema.Parser().parse(new File(schemaPath + "/" + "mapdata.avsc"));
            List<GenericData.Record> recordList = generator.generateRecords(schema, NUM_RECORDS);

            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withConf(new Configuration())
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build()) {

                for (GenericData.Record record : recordList) {
                    writer.write(record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
