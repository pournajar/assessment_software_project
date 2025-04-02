package com.asessment.app.util;

import java.io.Serializable;

public class Player implements Serializable {
    private String name;
    private Integer score;
    private String nationality;
    private Long timestamp;

    public Player() {}

    public Player(String name, int score,String nationality) {
        this.name = name;
        this.score = score;
        this.nationality = nationality;
        this.timestamp= System.currentTimeMillis();
    }

    public String getName() {
        return name;
    }

    public int getScore() {
        return score;
    }

    public String getNationality() {
        return nationality;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Name: " + name + ", Score: " + score + ", Nationality: " + nationality;
    }
}

