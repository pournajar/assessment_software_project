package com.asessment.app.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SampleDataTest {

    @Test
    public void testInitialPeople() {
        Map<String, String>[] people = SampleData.InitialPeople();
        List<String> names = Arrays.stream(SampleData.NAMES).collect(Collectors.toList());
        List<String> nationalities = Arrays.stream(SampleData.NATIONALITY).collect(Collectors.toList());
        for (int i = 0; i < people.length; i++) {
            assertTrue(people[i].get("name").equals(names.get(i)));
            assertTrue(people[i].get("nationality").equals(nationalities.get(i)));
        }
    }

}
