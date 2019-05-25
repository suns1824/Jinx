package com.raysurf.client.util;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class IOUtilsTest {

    @Test
    public void tryParseString() {
        InputStream is = null;
        try {
            File file = new File("src/main/resources/note/Readme_2");
            is = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String result = "fjasdlkjoadvjidscsjdlk;csldcavkajsd;faslmskacmdsmmcssdm;vmsdalkmfifsjl;smackdmavkmbjjfasidfoisdias;dlklassssssssssssssssssssssmmfds" +
                "dsf;ajlkfdiofaejavmlknvfadnvsdjfisoij ffamdskldmacmcdcaddcd.amcsdfafeop" +
                "davnoioj vvms" +
                "dfnaoewa jioavjsd;.adddsopfidfsovfavldsvnflaf" +
                "faijofevmsdkmkvmad;sf.vdmvlvmsdds" +
                "sdaflmvioaovbbldm.c" +
                "dfasddlmvibad";
        String str = null;
        try {
            str = IOUtils.tryParseString(is, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue(str.equals(result));
    }
}