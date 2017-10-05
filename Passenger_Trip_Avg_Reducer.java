package q1;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Passenger_Trip_Avg_Reducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
	float totPassngr = 0;
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
	IOException, InterruptedException { 
		int c = 0;
		if(key.find("Passenger")>0)      // add number of passengers
		{
			for (IntWritable value : values)
			{
				c+=value.get();	
			}
			totPassngr=c;
			context.write(key, new FloatWritable(c));
		}
		else
		{
			for (IntWritable val : values)
			{
				c+=val.get();	
			}

			context.write(key, new FloatWritable(c));
			String [] passngrkey= key.toString().split("_");
			//calculate average number of passengers
			context.write(new Text(passngrkey[0]+"_PassengerAverage"), new FloatWritable(totPassngr/c));
		}


	}
}