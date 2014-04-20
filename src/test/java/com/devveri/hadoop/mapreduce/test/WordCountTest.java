package com.devveri.hadoop.mapreduce.test;

import static com.devveri.hadoop.mapreduce.util.IOUtil.readFile;
import static com.devveri.hadoop.mapreduce.util.IOUtil.parseOutput;
import static com.devveri.hadoop.mapreduce.util.IOUtil.delete;

import static org.junit.Assert.*;

import com.devveri.hadoop.mapreduce.tool.WordCount;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

@Ignore
public class WordCountTest {

    private static final String INPUT = "src/test/resources/word_count.txt";
    private static final String OUTPUT = "/tmp/mapreduce-wordcount";
    private static final String OUTPUT_FILE = OUTPUT + "/part-r-00000";

    @Test
    public void testWordCount() throws Exception {
        // run the job
        WordCount wordCount = new WordCount();
        wordCount.run(new String[] {INPUT, OUTPUT});

        // assert output
        String output = readFile(new File(OUTPUT_FILE));
        Map<String, String> map = parseOutput(output);
        assertEquals("1", map.get("hadoop"));
        assertEquals("2", map.get("hello"));
        assertEquals("1", map.get("mapreduce"));

        // remove output
        delete(new File(OUTPUT));
    }

}
