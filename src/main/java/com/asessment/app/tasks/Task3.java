package com.asessment.app.tasks;

import com.asessment.app.util.CLI;
import com.asessment.app.util.ConfigReader;
import com.asessment.app.util.DataBaseHelper;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Task3 {

    public static void main(String[] args) throws Exception {

        // Initialize the parameters
        final CLI params = CLI.fromArgs(args);
        final long rate = 3L;
        final String inputData = "data.txt";

        String dbUrl = ConfigReader.get("db.url");
        String hostname = ConfigReader.get("db.hostname");
        int port = Integer.valueOf( ConfigReader.get("db.port"));
        String dbUser = ConfigReader.get("db.username");
        String dbPassword = ConfigReader.get("db.password");
        String sourceDb = ConfigReader.get("db.name");
        String driverName = ConfigReader.get("db.driver");

        DataBaseHelper dataBaseHelper = new DataBaseHelper();
        // Uncomment this to initialize database with input data
        // dataBaseHelper.InitData(env, inputData);


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .databaseList(sourceDb) // set captured database
                .tableList(sourceDb+".player") // set captured table
                .username(dbUser)
                .password(dbPassword)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");

    }


}
