package q2;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TripDistance_Average_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	float totDist = 0;
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException { 
		float c = 0;
		if(key.find("Distance")>0)  // to ensure correct key
		{
			for (FloatWritable val : values)
			{
				c+=val.get();	
			}
			totDist=c;  // calculate total distance
			context.write(key, new FloatWritable(c));
		}
		else
		{
			for (FloatWritable value : values)
			{
				c+=value.get();	
			}

			context.write(key, new FloatWritable(c));
			String [] pasKey= key.toString().split("_");
			context.write(new Text(pasKey[0]+"_Average_Distance"), new FloatWritable(totDist/c));
		}


	}
}