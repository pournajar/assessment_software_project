package com.asessment.app.util;

import org.apache.flink.api.java.tuple.Tuple4;

public class PlayerTuple extends Tuple4<String, Integer, String, Long> {
    public PlayerTuple() {
    }

    public PlayerTuple(String name, int score, String nationality, Long timestamp) {
        this.f0 = name;
        this.f1 = score;
        this.f2 = nationality;
        this.f3 = timestamp;
    }
}
