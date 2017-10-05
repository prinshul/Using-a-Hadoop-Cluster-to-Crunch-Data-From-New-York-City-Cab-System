package q1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Passenger_Trip_Avg_Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
 private IntWritable result = new IntWritable();
 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
IOException, InterruptedException {   
 int sum = 0;
 for (IntWritable val : values) {
 sum += val.get();   // add the same keys. This makes effective disk operations and makes the task easier for reducer. 
 }
 result.set(sum);
 context.write(key, result);
 }
}