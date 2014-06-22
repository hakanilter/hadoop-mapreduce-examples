package com.devveri.hadoop.mapreduce.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * User: hilter
 * Date: 22/06/14
 * Time: 13:35
 */
public class StockExampleTool extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job();
        job.setJobName("Stock Example");
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(StockExampleMapper.class);
        job.setReducerClass(StockExampleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StockExampleTool(), args);
        System.exit(exitCode);
    }

    /**
     * Mapper
     */
    static class StockExampleMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // use only valid lines
            if (!line.startsWith("NYSE")) {
                context.getCounter("Statistics", "Skipped Line").increment(1L);
                return;
            }

            // parse values
            String[] columns = line.split("\t");

            // select necessary values
            String stockSymbol = columns[1];
            float stockPriceHigh = Float.parseFloat(columns[4]);

            context.write(new Text(stockSymbol), new FloatWritable(stockPriceHigh));
        }

    }

    /**
     * Reducer
     */
    static class StockExampleReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            // iterate over values and find max
            float max = 0;
            for (FloatWritable value : values) {
                if (value.get() > max) {
                    max = value.get();
                }
            }

            // write max value for current stock symbol
            context.write(key, new FloatWritable(max));
        }

    }

}
