package org.donghoon.json;

import com.google.gson.*;
import org.donghoon.entity.MapData;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JsonFileReader {
    String path;

    public JsonFileReader(String path) {
        this.path = path;
    }

    public List<MapData> read() {
        List<MapData> records = new ArrayList<>();
        Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, (JsonDeserializer) (json, typeOfT, context) -> {
            return new Date(json.getAsLong());
        }).create();

        try {
            Reader reader = new FileReader(path);
            JsonArray value = JsonParser.parseReader(reader).getAsJsonArray();
            for (JsonElement je : value) {
                MapData record = gson.fromJson(je, MapData.class);
                records.add(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return records;
    }
}
