package q3;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Payment_Type_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	int cash=0;
	int credit=0;
	boolean flag=true;
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
	IOException, InterruptedException { 
		 		int sumVal = 0;
		 		for (IntWritable val : values)
				{
					sumVal+=val.get();	
				}
		 		if(key.toString().equalsIgnoreCase("csh"))
		 		{
		 			cash+=sumVal;
		 			
		 		}
		 		else if(key.toString().equalsIgnoreCase("cre"))
		 		{
		 			credit+=sumVal;
		 			
		 		}
		 		else
		 		{
		 			if(flag)
		 			{
		 			context.write(new Text("Cash"), new IntWritable(cash));
		 			context.write(new Text("Credit"), new IntWritable(credit));
		 			flag=false;
		 			}
		 			context.write(key, new IntWritable(sumVal));
		 		}
		 	}
		 	
			 
					
}