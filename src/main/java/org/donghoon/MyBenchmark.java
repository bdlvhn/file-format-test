/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.donghoon;

import org.apache.hadoop.fs.Path;
import org.donghoon.entity.MapData;
import org.donghoon.json.JsonFileReader;
import org.donghoon.json.JsonFileWriter;
import org.donghoon.orc.OrcFileReader;
import org.donghoon.orc.OrcFileWriter;
import org.donghoon.parquet.ParquetFileReader;
import org.donghoon.parquet.ParquetFileWriter;
import org.donghoon.util.Generator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.donghoon.util.Constants.NUM_RECORDS;
import static org.donghoon.util.Constants.outputPath;

public class MyBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        volatile Generator generator = new Generator();
        Path jsonPath = new Path(outputPath + "/" + NUM_RECORDS + "/" + "map_data" + ".json");
        Path parquetPath = new Path(outputPath + "/" + NUM_RECORDS + "/" + "map_data" + ".parquet");
        Path orcPath = new Path(outputPath + "/" + NUM_RECORDS + "/" +  "map_data" + ".orc");
    }

//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void measureJsonWriteTime(BenchmarkState state) {
//        JsonFileWriter.write(state.generator);
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void measureParquetWriteTime(BenchmarkState state) {
//        ParquetFileWriter.write(state.generator);
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void measureORCWriteTime(BenchmarkState state) {
//        OrcFileWriter.write(state.generator);
//    }

//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void measureJsonReadTime(BenchmarkState state) {
//        JsonFileReader jsonFileReader = new JsonFileReader(state.jsonPath.toString());
//        List<MapData> jsonRecords = jsonFileReader.read();
//        for (MapData jsonRecord : jsonRecords) {
//            System.out.println("jsonRecord = " + jsonRecord);
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void measureParquetReadTime(BenchmarkState state) {
//        ParquetFileReader parquetFileReader = new ParquetFileReader(state.parquetPath.toString());
//        List<MapData> parquetRecords = parquetFileReader.read();
//        for (MapData parquetRecord : parquetRecords) {
//            System.out.println("parquetRecord = " + parquetRecord);
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void measureORCReadTime(BenchmarkState state) {
//        OrcFileReader orcFileReader = new OrcFileReader(state.orcPath.toString());
//        List<MapData> orcRecords = orcFileReader.read();
//        for (MapData orcRecord : orcRecords) {
//            System.out.println("orcRecord = " + orcRecord);
//        }
//    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
