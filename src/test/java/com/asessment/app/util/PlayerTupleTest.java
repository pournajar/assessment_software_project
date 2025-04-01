package com.asessment.app.util;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class PlayerTupleTest {
    private static final String PLAYER_NAME = "PlayerName";
    private static final Integer PLAYER_SCORE = 10;
    private static final String PLAYER_NATIONALITY = "Country";
    private static final Long PLAYER_TIMESTAMP = 1234567890123L;

    @Test
    public void testConstructor() {
        PlayerTuple playerTuple = new PlayerTuple(PLAYER_NAME, PLAYER_SCORE, PLAYER_NATIONALITY, PLAYER_TIMESTAMP);
        assertEquals(PLAYER_NAME, playerTuple.getField(0));
        assertEquals(PLAYER_SCORE, playerTuple.getField(1));
        assertEquals(PLAYER_NATIONALITY, playerTuple.getField(2));
        assertEquals(PLAYER_TIMESTAMP, playerTuple.getField(3));
    }

    @Test
    public void testGetFieldByPosition() {
        PlayerTuple playerTuple = new PlayerTuple(PLAYER_NAME, PLAYER_SCORE, PLAYER_NATIONALITY, PLAYER_TIMESTAMP);

        assertEquals(PLAYER_NAME, playerTuple.getField(0));
        assertEquals(PLAYER_SCORE, playerTuple.getField(1));
        assertEquals(PLAYER_NATIONALITY, playerTuple.getField(2));
        assertEquals(PLAYER_TIMESTAMP, playerTuple.getField(3));
    }

    @Test
    public void testGetFieldByName() {
        PlayerTuple playerTuple = new PlayerTuple(PLAYER_NAME, PLAYER_SCORE, PLAYER_NATIONALITY, PLAYER_TIMESTAMP);

        assertEquals(PLAYER_NAME, playerTuple.getField(0));
        assertEquals(PLAYER_SCORE, playerTuple.getField(1));
        assertEquals(PLAYER_NATIONALITY, playerTuple.getField(2));
        assertEquals(PLAYER_TIMESTAMP, playerTuple.getField(3));
    }
}