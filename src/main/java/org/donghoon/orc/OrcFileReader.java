package org.donghoon.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.donghoon.entity.MapData;

import java.util.*;

import static org.donghoon.util.Constants.BATCH_SIZE;

public class OrcFileReader {
    String path;

    public OrcFileReader(String path) {
        this.path = path;
    }

    public List<MapData> read() {
        List<MapData> records = new LinkedList<>();

        try (Reader reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(new Configuration()))) {
            TypeDescription schema = reader.getSchema();

            try (RecordReader value = reader.rows(reader.options())) {
                VectorizedRowBatch batch = reader.getSchema().createRowBatch(BATCH_SIZE);
                LongColumnVector logtimeColumnVector = (LongColumnVector) batch.cols[0];
                BytesColumnVector timezoneColumnVector = (BytesColumnVector) batch.cols[1];
                LongColumnVector logidColumnVector = (LongColumnVector) batch.cols[2];
                LongColumnVector seqColumnVector = (LongColumnVector) batch.cols[3];
                BytesColumnVector infoColumnVector = (BytesColumnVector) batch.cols[4];
                BytesColumnVector plogdateColumnVector = (BytesColumnVector) batch.cols[5];

                while (value.nextBatch(batch)) {
                    for (int rowNum = 0; rowNum < batch.size; rowNum++) {
                        MapData record = MapData.builder()
                                .logtime(new Date(logtimeColumnVector.vector[rowNum]))
                                .timezone(timezoneColumnVector.toString(rowNum))
                                .logid((int) logidColumnVector.vector[rowNum])
                                .seq((int) seqColumnVector.vector[rowNum])
                                .info(infoColumnVector.toString(rowNum))
                                .plogdate(plogdateColumnVector.toString(rowNum))
                                .build();

                        records.add(record);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return records;
    }
}
