package com.asessment.app.tasks;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class TaskTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define two DataStreams
        DataStream<Tuple2<Integer, String>> nameStream = env.fromElements(
                Tuple2.of(1, "Alex"), Tuple2.of(2, "Peter"), Tuple2.of(3, "Josef"), Tuple2.of(4, "Max"), Tuple2.of(5, "Peter")
        );

        DataStream<Tuple2<Integer, Integer>> scoreStream = env.fromElements(
                Tuple2.of(1, 7), Tuple2.of(2, 2), Tuple2.of(3, 8), Tuple2.of(4, 4), Tuple2.of(5, 2)
        );

        // Connect the two streams
        ConnectedStreams<Tuple2<Integer, String>, Tuple2<Integer, Integer>> connectedStreams = nameStream.connect(scoreStream);

        // Apply a CoFlatMap function with KeyedState
        DataStream<Tuple2<String, Integer>> joinedStream = connectedStreams
                .keyBy(name -> name.f0, score -> score.f0)  // Key both streams by the ID
                .flatMap(new RichCoFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, Integer>, Tuple2<String, Integer>>() {
                    private transient MapState<Integer, String> nameState;
                    private transient MapState<Integer, Integer> scoreState;

                    @Override
                    public void open(Configuration parameters) {
                        nameState = getRuntimeContext().getMapState(new MapStateDescriptor<>("names", Integer.class, String.class));
                        scoreState = getRuntimeContext().getMapState(new MapStateDescriptor<>("scores", Integer.class, Integer.class));
                    }

                    @Override
                    public void flatMap1(Tuple2<Integer, String> nameTuple, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int id = nameTuple.f0;
                        nameState.put(id, nameTuple.f1);

                        if (scoreState.contains(id)) {
                            out.collect(new Tuple2<>(nameTuple.f1, scoreState.get(id)));
                            scoreState.remove(id);  // Cleanup after emitting
                            nameState.remove(id);
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<Integer, Integer> scoreTuple, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int id = scoreTuple.f0;
                        scoreState.put(id, scoreTuple.f1);

                        if (nameState.contains(id)) {
                            out.collect(new Tuple2<>(nameState.get(id), scoreTuple.f1));
                            scoreState.remove(id);
                            nameState.remove(id);
                        }
                    }
                });

        // Print the joined stream
        joinedStream.print("Joined Stream:");

        // Execute Flink Job
        env.execute("Reliable Join with State");
    }
}
