import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        // Set default number of reducers to 1
        int numReducers = 1;

        if (args.length > 2) {
            numReducers = Integer.parseInt(args[2]);
        }

        Job job = Job.getInstance(config);
        job.setJarByClass(Driver.class);

        job.setMapperClass(CarMapper.class);
        job.setReducerClass(CarReducer.class);
        job.setCombinerClass(CarCombiner.class);
        job.setPartitionerClass(CarPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setNumReduceTasks(numReducers);
        job.waitForCompletion(true);
    }
}