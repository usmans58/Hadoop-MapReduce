import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
        sum += val.get();
    }
    String[] authors = key.toString().split(",");
    String authorPair = authors[0] + " " + authors[1];
    context.write(new Text(authorPair + " : " + sum), null);
}
}

