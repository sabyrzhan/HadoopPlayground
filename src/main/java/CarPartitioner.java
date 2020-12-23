import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CarPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        System.out.println("Number of partitions: " + numPartitions);
        char startsWith = text.toString().toLowerCase().charAt(0);
        return startsWith <= 'm' ? 0 : 1;
    }
}
