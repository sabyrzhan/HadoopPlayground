import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CarCounterMrProgram extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        CarInputMRGenerator.createTestData();
        ToolRunner.run(new CarCounterMrProgram(), args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://192.168.0.10:9000");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.address", "192.168.0.10:8032");
        configuration.set("yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
                + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
                + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
                + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");

        Job job = Job.getInstance(configuration, "CarCounter");
        job.setJarByClass(CarCounterMrProgram.class);

        Path input = new Path("output/cars.data");
        Path output = new Path("output/results");

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);
        System.out.println("Job completed");

        return 0;
    }
}
