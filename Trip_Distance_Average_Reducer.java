package q5;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Trip_Distance_Average_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	float tripDistance = 0;
	float totalTripCounts=0;
	float hrstripDist=0;
	float hrstripCnt=0;
	boolean flag=true;
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
	
		 		float sumVal = 0;
		 		if(key.find("TotalTripNumber")>0)
		 		{
		 			for (FloatWritable val : values)
					{
						sumVal+=val.get();
						
					}
		 			hrstripCnt=sumVal;
		 		}
				if(key.find("TotalTripNumber")==-1)
				{
					
					for (FloatWritable val : values)
					{
						sumVal+=val.get();		
					}
					if(key.find("TripDistance")>0)
						context.write(new Text(key+"_Avg"),new FloatWritable(sumVal/hrstripCnt));
					else
						context.write(key,new FloatWritable(sumVal));
				}
				
				if(key.equals(new Text("Trip Distance")))
				{		
					tripDistance=sumVal;
				}
				if(key.equals(new Text("Total Number of Trip")))
				{
					totalTripCounts=sumVal;
				}
				if(flag && tripDistance>0 &&totalTripCounts>0)
				{
					flag=false;
					context.write(new Text("Average Trip Distance") ,new FloatWritable(tripDistance/totalTripCounts));
				}

			
	 }
}