package com.asessment.app.util;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PlayerTest {
    private static final String PLAYER_NAME = "PlayerName";
    private static final int PLAYER_SCORE = 10;
    private static final String PLAYER_NATIONALITY = "Country";
    private static final long PLAYER_TIMESTAMP = 1234567890123L;

    @Test
    public void testConstructor() {
        Player player = new Player(PLAYER_NAME, PLAYER_SCORE, PLAYER_NATIONALITY);
        assertEquals(PLAYER_NAME, player.getName());
        assertEquals(PLAYER_SCORE, player.getScore());
        assertEquals(PLAYER_NATIONALITY, player.getNationality());
        assertNotNull(player.getTimestamp());
    }

    @Test
    public void testSetTimestamp() {
        Player player = new Player(PLAYER_NAME, PLAYER_SCORE, PLAYER_NATIONALITY);
        player.setTimestamp(PLAYER_TIMESTAMP);
        assertEquals(PLAYER_TIMESTAMP, player.getTimestamp());
    }

    @Test
    public void testToString() {
        Player player = new Player(PLAYER_NAME, PLAYER_SCORE, PLAYER_NATIONALITY);
        String expectedString = "Name: " + PLAYER_NAME + ", Score: " + PLAYER_SCORE + ", Nationality: " + PLAYER_NATIONALITY;
        assertEquals(expectedString, player.toString());
    }
}