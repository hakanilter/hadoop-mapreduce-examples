package com.devveri.hadoop.mapreduce.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: hilter
 * Date: 2/3/14
 * Time: 9:34 AM
 */
public final class IOUtil {

    public static String readFile(File file) throws IOException {
        StringBuilder buffer = new StringBuilder();
        BufferedReader br = null;

        try {
            String line;
            br = new BufferedReader(new FileReader(file));
            while ((line = br.readLine()) != null) {
                buffer.append(line);
                buffer.append("\n");
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }

        return buffer.toString();
    }

    public static Map<String, String> parseOutput(String output) {
        Map<String, String> map = new HashMap<String, String>();
        for (String line : output.split("\n")) {
            String[] parsed = line.split("\t");
            if (parsed.length == 2) {
                map.put(parsed[0], parsed[1]);
            }
        }

        return map;
    }

    public static void delete(File file) throws IOException {
        if (file.isDirectory()) {
            if (file.list().length == 0) {
                file.delete();
            } else {
                String files[] = file.list();
                for (String temp : files) {
                    File fileDelete = new File(file, temp);
                    delete(fileDelete);
                }

                if (file.list().length == 0) {
                    file.delete();
                }
            }
        } else {
            file.delete();
        }
    }

}
