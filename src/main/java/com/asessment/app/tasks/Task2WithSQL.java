package com.asessment.app.tasks;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Task2WithSQL {
    public static void main(String[] args) throws Exception {


        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Sample data stream of Tuple3 <name, nationality, score>
        DataStream<Tuple3<String, Integer, String>> playerStream = env.fromElements(
                new Tuple3<>("Alex", 10, "USA"),
                new Tuple3<>("Peter", 20, "USA"),
                new Tuple3<>("Josef", 30, "Austria"),
                new Tuple3<>("Alex", 22, "USA"),
                new Tuple3<>("Max", 40, "Austria"),
                new Tuple3<>("Peter", 20, "USA")
        );


        Table playerTable = tableEnv.fromDataStream(playerStream, $("name")
                , $("score"), $("nationality"));

        // Task2-O3: Data Grouping by Nationality
        // Task2-O4: Average Score Calculation
        Table avgScoreByNationality = playerTable
                .groupBy($("nationality"))
                .select($("nationality"), $("score").avg().as("avg_score"));

        avgScoreByNationality.execute().print();

        // Task2-O5: Highest Score Retrieval
        Table maxScoreByNationality = playerTable
                .groupBy($("nationality"))
                .select($("nationality"), $("score").max().as("max_score"));

        maxScoreByNationality.execute().print();

        // Task2-O6: Display Average and Top player
        Table avgScoreByNationalityPerPlayer = playerTable
                .groupBy($("nationality"), $("name"))
                .select($("nationality"), $("name"), $("score").avg().as("avg_score_player"));
        avgScoreByNationalityPerPlayer.execute().print();

        Table maxScoreByNationalityPerPlayer = playerTable
                .groupBy($("nationality"), $("name"))
                .select($("nationality"), $("name"), $("score").max().as("max_score_player"));
        maxScoreByNationalityPerPlayer.execute().print();


        tableEnv.createTemporaryView("PlayerTable", playerTable);


        // Task2-O7: Overall Best Group
        Table resultTable = tableEnv.sqlQuery(
                "WITH AvgScore AS ( " +
                        "   SELECT nationality, AVG(score) AS avg_score " +
                        "   FROM PlayerTable " +
                        "   GROUP BY nationality " +
                        ") " +
                        "SELECT nationality, avg_score " +
                        "FROM AvgScore " +
                        "WHERE avg_score = (SELECT MAX(avg_score) FROM AvgScore)"
        );

        resultTable.execute().print();

        // Task2-O7: Top Three Individuals

        Table topThreePlayers = tableEnv.sqlQuery(
                "SELECT name, nationality, score " +
                        "FROM PlayerTable " +
                        "ORDER BY score DESC " +
                        "LIMIT 3"
        );

        topThreePlayers.execute().print();

        /*
        another way for topThreePlayers
        Table topThreePlayers = playerTable
                .orderBy($("score").desc())  // Order by score descending
                .fetch(3)                    // Limit to top 3 players
                .select($("name"), $("nationality"), $("score"));

        topThreePlayers.execute().print();
        */
    }// end of main
}
