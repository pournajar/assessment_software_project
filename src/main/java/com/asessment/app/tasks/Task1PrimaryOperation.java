/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.asessment.app.tasks;

import com.asessment.app.util.PlayerTuple;
import com.asessment.app.util.SampleData;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.table.api.*;
import org.apache.flink.util.Collector;
//import org.apache.flink.util.ParameterTool;


import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;


public class Task1PrimaryOperation {

    public static void main(String[] args) throws Exception {
        // Initialize the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);
        final boolean fileOutput = params.has("output");

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println(
                "To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");


        // get stream execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // generate initial players with name and nationality
        SampleData.InitialPeople();

        // create data stream of playerTuple, using getPlayerTupleGeneratorSource function
        DataStream<PlayerTuple> olympicPlayers =
                env.fromSource(
                        SampleData.getPlayerTupleGeneratorSource(rate),
                        IngestionTimeWatermarkStrategy.create(),
                        "Player Data Generator")
                        .setParallelism(1);

        int evictionSec = 10;
        double triggerSec = 10;
        // create keyed stream of olympicPlayers for testing topScore
        DataStream<PlayerTuple> keyedStream = olympicPlayers
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PlayerTuple>forMonotonousTimestamps()
                                .withTimestampAssigner((player, ts) -> player.f3))
                .keyBy(value -> value.f0);

        // find player with top score among all players
        DataStream<PlayerTuple> topScore =
                keyedStream
                        .windowAll(GlobalWindows.create())
                        .evictor(TimeEvictor.of(Duration.ofSeconds(evictionSec)))
                        .trigger(
                                DeltaTrigger.of(
                                        triggerSec,
                                        new DeltaFunction<PlayerTuple>() {
                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public double getDelta(
                                                    PlayerTuple oldDataPoint,
                                                    PlayerTuple newDataPoint) {
                                                return newDataPoint.f3 - oldDataPoint.f3;
                                            }
                                        },
                                        olympicPlayers.getType()
                                                .createSerializer(
                                                        env.getConfig().getSerializerConfig())))
                        .maxBy(1);


        DataStream<PlayerTuple> uniquePlayerDataStream = olympicPlayers
                .keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<>() {
                    @Override
                    public void processElement(PlayerTuple player, Context ctx, Collector<PlayerTuple> collector) throws Exception {
                        PlayerTuple previousPlayer = previousPlayerState.value();
                        if (previousPlayer == null) {
                            collector.collect(player);
                            previousPlayerState.update(player);
                        } else {
                            if (previousPlayer.f0 != player.f0 && previousPlayer.f2 != player.f2) {
                                collector.collect(player);
                                previousPlayerState.update(player);
                            }
                        }
                    }

                    private transient ValueState<PlayerTuple> previousPlayerState;

                    @Override
                    public void open(OpenContext openContext) {
                        ValueStateDescriptor<PlayerTuple> previousIbedRecStateDescriptor =
                                new ValueStateDescriptor<>("previousOrderState", TypeInformation.of(PlayerTuple.class));
                        previousPlayerState = getRuntimeContext().getState(previousIbedRecStateDescriptor);
                    }

                });


        if (fileOutput) {
            // print top score player
            topScore.sinkTo(FileSink.<PlayerTuple>forRowFormat(
                    new Path(params.get("output")),
                    new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                    .withRolloverInterval(Duration.ofSeconds(10))
                                    .build())
                    .build())
                    .name("output-top-score");

            // print unique players
            uniquePlayerDataStream.sinkTo(FileSink.<PlayerTuple>forRowFormat(
                    new Path(params.get("output")),
                    new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                    .withRolloverInterval(Duration.ofSeconds(10))
                                    .build())
                    .build())
                    .name("output-unique-player");
        } else {

            // print top score player
            // topScore.print().setParallelism(1);

            // print unique player
            uniquePlayerDataStream.print();
        }

        // execute program
        env.execute("Olympic Data Management Example");

        // Uncomment this to test tableEnvironment features
        // TestTableEnvironment();
    }


    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {
        }

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }

    public static void TestTableEnvironment() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // Fully programmatic
        env.fromValues(1).execute().print();

        // Flink SQL is included
        env.sqlQuery("SELECT 1").execute().print();

        // The API feels like SQL (built-in functions, data types, ...)
        Table table = env.fromValues("1").as("c").select($("c").cast(DataTypes.STRING()));

        // Everything is centered around Table objects, conceptually SQL views (=virtual tables)
        env.createTemporaryView("InputTable", table);

        // Catalogs and metadata management are the foundation
        env.from("InputTable").insertInto(TableDescriptor.forConnector("blackhole").build()).execute();

        // Let's get started with unbounded data...
        env.from(
                TableDescriptor.forConnector("datagen")
                        .schema(
                                Schema.newBuilder()
                                        .column("uid", DataTypes.BIGINT())
                                        .column("s", DataTypes.STRING())
                                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                                        .build())
                        .build())
                .execute()
                .print();
    }

}
