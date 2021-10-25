package com.datastax.pulsar.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageRouterTests {

    @Test
    public void testTokens() {
        assertEquals(65535, ((short)(Long.MAX_VALUE >>> 48)) + Short.MAX_VALUE + 1);
        assertEquals(0, ((short)(Long.MIN_VALUE >>> 48)) + Short.MAX_VALUE + 1);
    }
}
