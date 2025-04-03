package com.asessment.app.tasks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class Task2 {


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FileSource<String> nameSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("./src/main/resources/names.txt")
        ).build();
        DataStream<String> nameStream = env.fromSource(
                nameSource,
                WatermarkStrategy.noWatermarks(),
                "names-input"
        );
        DataStream<Tuple2<Integer, String>> indexedNames = nameStream
                .flatMap((String line, Collector<Tuple2<Integer, String>> out) -> {
                    List<String> names = Arrays.asList(line.replaceAll("\"", "").split(" "));
                    IntStream.range(0, names.size())
                            .mapToObj(i -> new Tuple2<>(i, names.get(i)))
                            .forEach(out::collect);
                }).returns(new TypeHint<Tuple2<Integer, String>>() {
                });

        indexedNames.print("Indexed Name Stream");


        FileSource<String> scoreSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("./src/main/resources/scores.txt")
        ).build();
        DataStream<String> scoreStream = env.fromSource(
                scoreSource,
                WatermarkStrategy.noWatermarks(),
                "scores-input"
        );
        DataStream<Tuple2<Integer, Integer>> indexedScores = scoreStream
                .flatMap((String line, Collector<Tuple2<Integer, Integer>> out) -> {
                    List<String> scores = Arrays.asList(line.split(" "));
                    IntStream.range(0, scores.size())
                            .mapToObj(i -> new Tuple2<>(i, Integer.parseInt(scores.get(i))))
                            .forEach(out::collect);
                }).returns(new TypeHint<Tuple2<Integer, Integer>>() {
                });

        indexedScores.print("Indexed Score Stream");

        FileSource<String> nationalitySource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("./src/main/resources/nationality.txt")
        ).build();
        DataStream<String> nationalityStream = env.fromSource(
                nationalitySource,
                WatermarkStrategy.noWatermarks(),
                "nationality-input"
        );
        DataStream<Tuple2<Integer, String>> indexedNationality = nationalityStream
                .flatMap((String line, Collector<Tuple2<Integer, String>> out) -> {
                    List<String> scores = Arrays.asList(line.split(" "));
                    IntStream.range(0, scores.size())
                            .mapToObj(i -> new Tuple2<>(i, scores.get(i)))
                            .forEach(out::collect);
                }).returns(new TypeHint<Tuple2<Integer, String>>() {
                });

        indexedNationality.print("Indexed Nationality Stream");

        ConnectedStreams<Tuple2<Integer, String>, Tuple2<Integer, Integer>>
                nameScoreConnected = indexedNames.connect(indexedScores);

        // Process the connected streams
        DataStream<Tuple3<Integer, String, Integer>> nameScoreStream = nameScoreConnected
                .keyBy(name -> name.f0, score -> score.f0)  // Key both streams by the ID
                .flatMap(new RichCoFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, Integer>, Tuple3<Integer, String, Integer>>() {
                    private transient MapState<Integer, String> nameState;
                    private transient MapState<Integer, Integer> scoreState;
                    private transient MapState<Integer, String> nationalityState;

                    @Override
                    public void open(Configuration parameters) {
                        nameState = getRuntimeContext().getMapState(new MapStateDescriptor<>("names", Integer.class, String.class));
                        scoreState = getRuntimeContext().getMapState(new MapStateDescriptor<>("scores", Integer.class, Integer.class));
                    }

                    @Override
                    public void flatMap1(Tuple2<Integer, String> nameTuple, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
                        int id = nameTuple.f0;
                        nameState.put(id, nameTuple.f1);

                        if (scoreState.contains(id)) {
                            out.collect(new Tuple3<>(id, nameTuple.f1, scoreState.get(id)));
                            scoreState.remove(id);  // Cleanup after emitting
                            nameState.remove(id);
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<Integer, Integer> scoreTuple, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
                        int id = scoreTuple.f0;
                        scoreState.put(id, scoreTuple.f1);

                        if (nameState.contains(id)) {
                            out.collect(new Tuple3<>(id, nameState.get(id), scoreTuple.f1));
                            scoreState.remove(id);
                            nameState.remove(id);
                        }
                    }
                });

        ConnectedStreams<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>>
                joinedConnected = nameScoreStream.connect(indexedNationality);

        DataStream<Tuple3<String, Integer, String>> joinedStream = joinedConnected
                .keyBy(nameScore -> nameScore.f0, nationality -> nationality.f0)
                .flatMap(new RichCoFlatMapFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, Tuple3<String, Integer, String>>() {
                    private transient MapState<Integer, Tuple3<String, Integer, String>> playerState;
                    private transient MapState<Integer, String> nationalityState;

                    @Override
                    public void open(Configuration parameters) {
                        playerState = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                                "players",
                                TypeInformation.of(Integer.class),
                                TypeInformation.of(new TypeHint<Tuple3<String, Integer, String>>() {
                                })
                        ));
//                        playerState = getRuntimeContext().getMapState(new MapStateDescriptor<>("players", Integer.class, Tuple3.class));
                        nationalityState = getRuntimeContext().getMapState(new MapStateDescriptor<>("nationality", Integer.class, String.class));
                    }

                    @Override
                    public void flatMap1(Tuple3<Integer, String, Integer> playerTuple, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        int id = playerTuple.f0;
                        playerState.put(id, new Tuple3<>(playerTuple.f1, null, null));

                        if (nationalityState.contains(id)) {
                            out.collect(new Tuple3<>(playerTuple.f1, playerTuple.f2, nationalityState.get(id)));
                            nationalityState.remove(id);
                            playerState.remove(id);
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<Integer, String> nationalityTuple, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        int id = nationalityTuple.f0;
                        nationalityState.put(id, nationalityTuple.f1);

                        if (playerState.contains(id)) {
                            Tuple3<String, Integer, String> playerData = playerState.get(id);
                            out.collect(new Tuple3<>(playerData.f2, playerData.f1, nationalityTuple.f1));
                            nationalityState.remove(id);
                            playerState.remove(id);
                        }
                    }
                });


        joinedStream.print("Joined Stream:");


        // Deduplicate the joined stream based on name
        DataStream<Tuple3<String, Integer, String>> deduplicatedJoinedStream = joinedStream
                .keyBy(tuple -> tuple.f0)
                .reduce((t1, t2) -> t1);
        deduplicatedJoinedStream.print("Deduplicate Joined Stream");


        // Group by nationality
        DataStream<Tuple3<String, Integer, String>> groupedByNationality = deduplicatedJoinedStream
                .keyBy(player -> player.f2)
                .reduce((player1, player2) -> new Tuple3<>(
                        player1.f0 + " - " + player2.f0,  // Nationality (same for both, so we take it from one of the players)
                        player1.f1 + player2.f1, // Aggregate the scores
                        player1.f2  // Nationality (same as above)
                ));

        groupedByNationality.print("Grouped by Nationality");

        env.execute("Task2");


    }// end of main


}
