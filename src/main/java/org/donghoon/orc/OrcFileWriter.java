package org.donghoon.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.donghoon.util.Generator;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.donghoon.util.Constants.*;

public class OrcFileWriter {

    public static void write(Generator generator) {
        try {
            Path path = new Path(outputPath + "/" + "map_data" + ".orc");
            Files.deleteIfExists(Paths.get(path.toString()));

            TypeDescription schema = TypeDescription.fromString("struct<logtime:date,timezone:string,logid:int,seq:int,info:string,plogdate:string>");
            List<String> fieldNames = schema.getFieldNames();
            List<TypeDescription> columnTypes =schema.getChildren();

            List<Map<String, Object>> data = generator.generateHashMap(NUM_RECORDS);

            VectorizedRowBatch batch = schema.createRowBatch();

            List<BiConsumer<Integer, Object>> consumers = new ArrayList<>(columnTypes.size());
            for (int i = 0; i < columnTypes.size(); i++) {
                TypeDescription type = columnTypes.get(i);
                ColumnVector vector = batch.cols[i];
                consumers.add(createColumnWriter(type, vector));
            }

            try (Writer writer = OrcFile.createWriter(path,
                    OrcFile.writerOptions(new Configuration())
                            .setSchema(schema))) {
                for (Map<String, Object> row : data) {
                    int rowNum = batch.size++;

                    for (int i = 0; i < fieldNames.size(); i++) {
                        consumers.get(i).accept(rowNum, row.get(fieldNames.get(i)));
                    }

                    if (batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
                if (batch.size != 0) {
                    writer.addRowBatch(batch);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BiConsumer<Integer, Object> createColumnWriter(TypeDescription description, ColumnVector columnVector) {
        String type = description.getCategory().getName();
        BiConsumer<Integer, Object> consumer;
        if ("tinyint".equals(type)) {
            consumer = (row, val) -> ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        } else if ("smallint".equals(type)) {
            consumer = (row, val) -> ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        } else if ("int".equals(type) || "date".equals(type)) {
            // Date is represented as int epoch days
            consumer = (row, val) -> ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        } else if ("bigint".equals(type)) {
            consumer = (row, val) -> ((LongColumnVector) columnVector).vector[row] = ((Number) val).longValue();
        } else if ("boolean".equals(type)) {
            consumer = (row, val) -> ((LongColumnVector) columnVector).vector[row] = (Boolean) val ? 1 : 0;
        } else if ("float".equals(type)) {
            consumer = (row, val) -> ((DoubleColumnVector) columnVector).vector[row] = ((Number) val).floatValue();
        } else if ("double".equals(type)) {
            consumer = (row, val) -> ((DoubleColumnVector) columnVector).vector[row] = ((Number) val).doubleValue();
        } else if ("decimal".equals(type)) {
            consumer = (row, val) -> ((DecimalColumnVector) columnVector).vector[row].set(HiveDecimal.create((BigDecimal) val));
        } else if ("string".equals(type) || type.startsWith("varchar") || "char".equals(type)) {
            consumer = (row, val) -> {
                byte[] buffer = val.toString().getBytes(StandardCharsets.UTF_8);
                ((BytesColumnVector) columnVector).setRef(row, buffer, 0, buffer.length);
            };
        } else if ("timestamp".equals(type)) {
            consumer = (row, val) -> ((TimestampColumnVector) columnVector).set(row, (Timestamp) val);
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
        return consumer;
    }
}
