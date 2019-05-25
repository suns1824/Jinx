package com.raysurf.client.util;

import org.junit.Test;
import static org.junit.Assert.*;

public class MessageDecoderTest {

    private static String testStr = "Hello World";
    private static String bytesStr = "SGVsbG8gV29ybGQ=";

    @Test
    public void decode() {
        byte[] bytes = MessageDecoder.decode(bytesStr);
        String str = new String(bytes);
        assertTrue(str.equals(testStr));
    }

}