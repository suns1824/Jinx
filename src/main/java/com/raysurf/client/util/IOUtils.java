package com.raysurf.client.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class IOUtils {

    public static String parseString(InputStream is, String charset) throws IOException {
        if (is != null && charset != null) {
            return tryParseString(is, charset);
        } else {
            throw new IllegalArgumentException("param error");
        }
    }

    public static String tryParseString(InputStream is, String ... charsets) throws IOException {
        if (is != null && charsets != null && charsets.length > 0) {
            BufferedInputStream bIs = null;
            BufferedOutputStream bOs = null;
            byte[] buffer = null;
            try {
                buffer = new byte[128];
                bIs = new BufferedInputStream(is);
                ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream(128);
                bOs = new BufferedOutputStream(outputBuffer);
                while (true) {
                    int len;
                    if ((len = bIs.read(buffer)) == -1) {
                        bOs.flush();
                        buffer = outputBuffer.toByteArray();
                        break;
                    }
                    bOs.write(buffer, 0, len);
                }
            } finally {
                close(bIs, bOs);
            }
            CharacterCodingException e = null;
            String[] arr = charsets;
            int len = arr.length;
            int i = 0;
            while (i < len) {
                String charset = arr[i];
                try {
                    CharsetDecoder decoder = Charset.forName(charset).newDecoder();
                    return decoder.decode(ByteBuffer.wrap(buffer)).toString();
                } catch (CharacterCodingException codingException) {
                    i++;
                }
            }
            throw e;
        } else
            throw new IllegalArgumentException("param error");
    }

    public static void close(Closeable... streams) {
        if (streams != null && streams.length != 0) {
            Closeable[] arr = streams;
            int len = arr.length;

            for(int i = 0; i < len; i++) {
                Closeable c = arr[i];
                try {
                    if (c != null) {
                        c.close();
                    }
                } catch (IOException e) {
                    ;
                }
            }
        }

    }
}
