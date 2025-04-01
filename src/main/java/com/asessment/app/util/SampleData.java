/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.asessment.app.util;

import com.asessment.app.tasks.Task1PrimaryOperation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Sample data for the {@link Task1PrimaryOperation} example.
 */
@SuppressWarnings("serial")
public class SampleData {

    static final String[] NAMES = {"Alex", "Peter", "Josef", "Max", "john", "Grace"};
    static final String[] NATIONALITY = {"USA", "Germany", "Italy", "Spain", "Greece", "Austria"};
    static Map<String, String>[] PEOPLE;
    static Player[] PLAYERS;
    static final int GRADE_COUNT = 5;
    static final int SCORE_MAX = 100;
    static final int SALARY_MAX = 10000;


    public static Map<String, String>[] InitialPeople() {
        PEOPLE = new HashMap[NAMES.length];

        for (int i = 0; i < NAMES.length; i++) {
            Map<String, String> person = new HashMap<>();
            person.put("name", NAMES[i]);
            person.put("nationality", NATIONALITY[i]);
            PEOPLE[i] = person;
        }
        return PEOPLE;
    }


    /**
     * Continuously generates (player name, score, nationality).
     */
    public static DataGeneratorSource<Player> getPlayerGeneratorSource(
            double elementsPerSecond) {
        return getTuplePlayerGeneratorSource(SCORE_MAX, elementsPerSecond);
    }

    public static DataGeneratorSource<Player> getTuplePlayerGeneratorSource(
            int maxValue, double elementsPerSecond) {
        final Random rnd = new Random();
        final GeneratorFunction<Long, Player> generatorFunction =
                index -> {
                    int randomIndex = rnd.nextInt(NAMES.length);
                    return new Player(PEOPLE[randomIndex].get("name"), rnd.nextInt(maxValue) + 1, PEOPLE[randomIndex].get("nationality"));
                };
        return new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(elementsPerSecond),
                TypeInformation.of(new TypeHint<Player>() {
                }));
    }

    public static DataGeneratorSource<PlayerTuple> getPlayerTupleGeneratorSource(
            double elementsPerSecond) {
        return getPlayerTupleGeneratorSource(SCORE_MAX, elementsPerSecond);
    }

    private static DataGeneratorSource<PlayerTuple> getPlayerTupleGeneratorSource(
            int maxValue, double elementsPerSecond) {
        final Random rnd = new Random();
        final GeneratorFunction<Long, PlayerTuple> generatorFunction =
                index -> {
                    int randomIndex = rnd.nextInt(NAMES.length);
                    return new PlayerTuple(PEOPLE[randomIndex].get("name"), rnd.nextInt(maxValue) + 1,
                            PEOPLE[randomIndex].get("nationality"), System.currentTimeMillis() );
                };
        return new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(elementsPerSecond),
                TypeInformation.of(new TypeHint<PlayerTuple>() {
                }));
    }

}
