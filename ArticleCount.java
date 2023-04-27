
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ArticleCount {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();   // Create a new Hadoop Configuration object
    Job job = Job.getInstance(conf, "Article Count");   // Create a new Hadoop Job object with the given Configuration object and job name
    job.setJarByClass(ArticleCount.class); //Main Class
    job.setMapperClass(ArticleCountMapper.class); //Mapper Class
    job.setReducerClass(ArticleCountReducer.class); //Reducer Class
    job.setOutputKeyClass(Text.class); //Datatype for Output
    job.setOutputValueClass(LongWritable.class); 
    FileInputFormat.addInputPath(job, new Path(args[0]));//Input Path (given in command)
    FileOutputFormat.setOutputPath(job, new Path(args[1]));//Output Path (given in command)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
