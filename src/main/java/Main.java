import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        MapDriver<LongWritable, Text, Text, IntWritable> driver =
                MapDriver.<LongWritable, Text, Text, IntWritable>newMapDriver()
                        .withMapper(new CarMapper())
                        .withInput(new LongWritable(0), new Text("BMW BMW Toyota"))
                        .withInput(new LongWritable(0), new Text("Rolls-Royce Honda Honda"))
                        .withOutput(new Text("bmw"), new IntWritable(1))
                        .withOutput(new Text("bmw"), new IntWritable(1))
                        .withOutput(new Text("toyota"), new IntWritable(1))
                        .withOutput(new Text("rolls-royce"), new IntWritable(1))
                        .withOutput(new Text("honda"), new IntWritable(1))
                        .withOutput(new Text("honda"), new IntWritable(1));

        driver.runTest();

        ReduceDriver<Text, IntWritable, Text, LongWritable> reducerDriver =
                ReduceDriver.<Text, IntWritable, Text, LongWritable>newReduceDriver()
                        .withReducer(new CarReducer())
                        .withInput(new Text("bmw"), List.of(new IntWritable(1), new IntWritable(2)))
                        .withInput(new Text("toyota"), List.of(new IntWritable(5), new IntWritable(5)))
                        .withInput(new Text("honda"), List.of(new IntWritable(5)))
                        .withOutput(new Text("bmw"), new LongWritable(3))
                        .withOutput(new Text("toyota"), new LongWritable(10))
                        .withOutput(new Text("honda"), new LongWritable(5));

        reducerDriver.runTest();
    }
}
