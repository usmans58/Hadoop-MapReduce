import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArticleCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString(); 
    String[] fields = line.split(","); //split string by comma
    
    if (fields.length == 4) {
      String year = fields[0]; //Get year
      String journal = fields[1]; //Get Journal
      context.write(new Text(year + "," + journal), new LongWritable(1)); //Key,Value Pair
    }
  }
}
