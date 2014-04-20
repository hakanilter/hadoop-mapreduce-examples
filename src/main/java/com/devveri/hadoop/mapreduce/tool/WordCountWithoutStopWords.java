package com.devveri.hadoop.mapreduce.tool;

import static com.devveri.hadoop.mapreduce.util.IOUtil.readFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * User: hilter
 * Date: 2/3/14
 * Time: 12:32 AM
 *
 * This example uses Hadoop Distributed Cache
 * Reference: http://developer.yahoo.com/hadoop/tutorial/module5.html#auxdata
 */
public class WordCountWithoutStopWords extends Configured implements Tool {

    public static final String LOCAL_STOPWORD_LIST = "src/test/resources/stop_words.txt";
    public static final String HDFS_STOPWORD_LIST = "/tmp/stop_words.txt";

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(WordCount.class);

        cacheStopWordList(job.getConfiguration());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountWithoutStopWordsMapper.class);
        job.setReducerClass(WordCountWithoutStopWordsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Loads stop words into hdfs and add it to distributed cache
     *
     * @param configuration Job configuration
     * @throws IOException
     */
    private void cacheStopWordList(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path(HDFS_STOPWORD_LIST);

        // upload the file to hdfs. Overwrite any existing copy.
        fs.copyFromLocalFile(false, true, new Path(LOCAL_STOPWORD_LIST), path);

        DistributedCache.addCacheFile(path.toUri(), configuration);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    /**
     * Mapper
     */
    static class WordCountWithoutStopWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Set<String> stopWords;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                Text word = new Text(tokenizer.nextToken());
                if (!stopWords.contains(word)) {
                    context.write(word, new IntWritable(1));
                }
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            String cacheName = new Path(HDFS_STOPWORD_LIST).getName();
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            if (null != cacheFiles && cacheFiles.length > 0) {
                for (Path cachePath : cacheFiles) {
                    if (cachePath.getName().equals(cacheName)) {
                        loadStopWords(cachePath);
                        break;
                    }
                }
            }
        }

        private void loadStopWords(Path cachePath) throws IOException {
            stopWords = new HashSet<String>();
            String text = readFile(new File(cachePath.toString()));
            for (String line : text.split("\n")) {
                stopWords.add(line);
            }
        }

    }

    /**
     * Reducer
     */
    static class WordCountWithoutStopWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }

}
