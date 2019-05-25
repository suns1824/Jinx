package com.raysurf.client.util;

import java.io.File;

public class AppUtils {
    public static final String parseJarVersion(Class<?> clz, String jarFilePrefix) {
        String path = clz.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path != null && !path.isEmpty()) {
            String version = null;
            try {
                File file = new File(path);
                if (file.exists() && file.isFile()) {
                    String fn = file.getName();
                    if (fn.startsWith(jarFilePrefix) && fn.endsWith(".jar")) {
                        version = fn.substring(jarFilePrefix.length(), fn.lastIndexOf("."));
                    }
                } else if (path.indexOf(jarFilePrefix) != -1 && path.indexOf(".jar!") != -1) {
                    String[] sa = path.split("[/]");
                    String fn = sa[sa.length - 1];
                    if (fn.startsWith(jarFilePrefix)) {
                        version = fn.substring(jarFilePrefix.length(), fn.lastIndexOf(46));
                    }
                }
            } catch (Throwable cause) {
                ;
            }
            return version;
        } else {
            return null;
        }
    }
}
