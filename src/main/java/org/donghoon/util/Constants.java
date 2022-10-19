package org.donghoon.util;

import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final String outputPath = "C:/Users/donghoon/Desktop/playground/jmh/file-format-test/output";
    public static final String schemaPath = "C:/Users/donghoon/Desktop/playground/jmh/file-format-test/schema";

    public static final Integer NUM_RECORDS = 10000;
    public static final int BATCH_SIZE = 2048;

    public static final List<String> timezones = Arrays.asList("Asia/Seoul", "Africa/Cairo","America/El_Salvador","America/Hermosillo","America/Mexico_City","America/Sao_Paulo");
    public static final List<Integer> logids = Arrays.asList(9000,9001,9002,10000,10001,10002,20000,20001,20002,300000);
}
