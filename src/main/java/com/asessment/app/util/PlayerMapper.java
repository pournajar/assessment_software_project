package com.asessment.app.util;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PlayerMapper extends RichFlatMapFunction<String, Player> {
    public int lineCount = 0;
    public List<String> names = null;
    public List<Integer> scores = null;
    public List<String> nationalities= null ;

    @Override
    public void flatMap(String line, Collector<Player> out) throws Exception {


        lineCount++;
        if (line.startsWith("Names:")) {
            names = Arrays.asList(line.replace("Names:", "").trim().replace("\"", "").split(" "));

        } else if (line.startsWith("Scores:")) {
            scores = Arrays.stream(line.replace("Scores:", "").trim().split(" "))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        } else if (line.startsWith("Nationality:")) {
            nationalities = Arrays.asList(line.replace("Nationality:", "").trim().replace("\"", "").split(" "));
        }

        // Once all lines are processed, emit Player objects
        if (lineCount == 3) {
            for (int i = 0; i < names.size(); i++) {
                out.collect(new Player(names.get(i), scores.get(i), nationalities.get(i)));
            }
        }
    }
}
