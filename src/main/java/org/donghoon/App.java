package org.donghoon;

import org.apache.hadoop.fs.Path;
import org.donghoon.entity.MapData;
import org.donghoon.json.JsonFileReader;
import org.donghoon.orc.OrcFileReader;
import org.donghoon.parquet.ParquetFileReader;
import org.donghoon.util.Generator;

import java.io.IOException;
import java.util.List;

import static org.donghoon.util.Constants.outputPath;

public class App {
    public static void main(String[] args) throws IOException {
        Generator generator = new Generator();

        Path jsonPath = new Path(outputPath + "/" + "map_data" + ".json");
        Path parquetPath = new Path(outputPath + "/" + "map_data" + ".parquet");
        Path orcPath = new Path(outputPath + "/" + "map_data" + ".orc");

//        // 1. write plain (JSON)
//        JsonWriter jsonWriter = new JsonWriter();
//        jsonWriter.write(generator);
//
//        // 2. write Parquet
//        ParquetFileWriter parquetFileWriter = new ParquetFileWriter();
//        parquetFileWriter.write(generator);
//
//        // 3. write ORC
//        OrcFileWriter orcFileWriter = new OrcFileWriter();
//        orcFileWriter.write(generator);


//        // 4. read JSON
//        JsonFileReader jsonFileReader = new JsonFileReader(jsonPath.toString());
//        List<MapData> jsonRecords = jsonFileReader.read();
//        for (MapData jsonRecord : jsonRecords) {
//            System.out.println("jsonRecord = " + jsonRecord);
//        }
//
//        // 5. read Parquet
//        ParquetFileReader parquetFileReader = new ParquetFileReader(parquetPath.toString());
//        List<MapData> parquetRecords = parquetFileReader.read();
//        for (MapData parquetRecord : parquetRecords) {
//            System.out.println("parquetRecord = " + parquetRecord);
//        }
//
//        // 6. read ORC
//        OrcFileReader orcFileReader = new OrcFileReader(orcPath.toString());
//        List<MapData> orcRecords = orcFileReader.read();
//        for (MapData orcRecord : orcRecords) {
//            System.out.println("orcRecord = " + orcRecord);
//        }

    }
}
