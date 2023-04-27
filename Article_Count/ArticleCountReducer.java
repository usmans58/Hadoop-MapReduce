import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ArticleCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long count = 0; //Adds count
    for (LongWritable value : values) {  // Iterate through the values and add up the counts
      count += value.get();
    }
    context.write(key, new LongWritable(count)); // Write the key-value pair to the context
  }
}
