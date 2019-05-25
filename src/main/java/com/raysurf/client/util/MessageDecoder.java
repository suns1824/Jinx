package com.raysurf.client.util;

import java.util.Base64;

public class MessageDecoder {
    private static final Base64.Decoder base64Decoder = Base64.getDecoder();
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    public MessageDecoder() {
    }

    public static byte[] decode(String str) {
        return base64Decoder.decode(str);
    }

}
