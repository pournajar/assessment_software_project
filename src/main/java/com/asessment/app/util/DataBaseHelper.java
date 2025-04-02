package com.asessment.app.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataBaseHelper {

    public void InitData(StreamExecutionEnvironment env, String fileName) {

        // Read players data from file
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path("./src/main/resources/" + fileName)).build();
        DataStream<String> fileLines = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");

        DataStream<Player> olympicPlayers = fileLines.flatMap(new PlayerMapper())
                .returns(TypeInformation.of(Player.class));

        // Define JDBC Sink to write players to MySQL
        olympicPlayers.addSink(JdbcSink.sink(
                "INSERT INTO player (name, score, nationality) VALUES (?, ?, ?)",
                (statement, player) -> {
                    statement.setString(1, player.getName());
                    statement.setInt(2, player.getScore());
                    statement.setString(3, player.getNationality());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/assessment")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("B@ran2378")
                        .build()
        ));
    }
}
