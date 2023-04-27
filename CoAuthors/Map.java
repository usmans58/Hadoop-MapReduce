import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;



public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private Text authorPair = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        if (fields.length < 4) {
            return;
        }
        String[] authors = fields[3].split(":");

        for (int i = 0; i < authors.length; i++) {
            for (int j = i + 1; j < authors.length; j++) {
                authorPair.set(authors[i] + "," + authors[j]);
                context.write(authorPair, ONE);
            }
        }
    }

      
  }
