package org.donghoon.json;

import org.apache.hadoop.fs.Path;
import org.donghoon.util.Generator;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.donghoon.util.Constants.NUM_RECORDS;
import static org.donghoon.util.Constants.outputPath;

public class JsonFileWriter {

    public static void write(Generator generator) {
        try {
            Path path = new Path(outputPath + "/" + "map_data" + ".json");
            Files.deleteIfExists(Paths.get(path.toString()));

            List<JSONObject> data = generator.generateJson(NUM_RECORDS);

            try (PrintWriter out = new PrintWriter(new FileWriter(String.valueOf(path)))) {
                out.write(data.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
