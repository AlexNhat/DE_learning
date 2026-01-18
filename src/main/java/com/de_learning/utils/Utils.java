package com.de_learning.utils;

import java.io.File;


public final class Utils {
    public static final String osSep = File.separator;
    public static final String projRoot = System.getProperty("user.dir");
    public static final String rawDataRoot = String.join(osSep, projRoot, "raw_data");
}
