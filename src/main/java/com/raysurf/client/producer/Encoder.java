package com.raysurf.client.producer;

import java.io.IOException;

public interface Encoder {
    String encode(Object msg) throws IOException;
}
