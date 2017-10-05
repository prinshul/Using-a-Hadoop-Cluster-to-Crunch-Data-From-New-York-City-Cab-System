package q5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Trip_Distance_Average_JobControl {
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job();
 job.setJarByClass(Trip_Distance_Average_JobControl.class);
 job.setMapperClass(Trip_Distance_Average_Mapper.class);
 job.setCombinerClass(Trip_Distance_Average_Combiner.class);
 job.setReducerClass(Trip_Distance_Average_Reducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(FloatWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}