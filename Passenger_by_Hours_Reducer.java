package q4;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Passenger_by_Hours_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	float tripPassenger = 0;
	float totalNumberTrip=0;
	float hoursTripCount=0;
	boolean flag=true;
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
	
		 		float sumVal = 0;
		 		if(key.find("TotalTripNumber")>0)  // to ensure correct key is present
		 		{
		 			for (FloatWritable val : values)
					{
						sumVal+=val.get();
						
					}
		 			hoursTripCount=sumVal;  // count the total
		 			//context.write(key,new FloatWritable(sumVal));
		 		}
				if(key.find("TotalTripNumber")==-1)   // to ensure key is present
				{
					
					for (FloatWritable val : values)
					{
						sumVal+=val.get();		
					}
					if(key.find("TripPassanger")>0)
						context.write(new Text(key+"_Avg"),new FloatWritable(sumVal/hoursTripCount));
					else
						context.write(key,new FloatWritable(sumVal));
				}
				
				if(key.equals(new Text("Total Trip Passanger")))   // assign value to 
				{		                                       //sum variable depending on the key
					tripPassenger=sumVal;
				}
				if(key.equals(new Text("Total Number of Trip")))
				{
					totalNumberTrip=sumVal;
				}
				if(flag && tripPassenger>0 &&totalNumberTrip>0)
				{
					flag=false;
					context.write(new Text("Average Trip Passanger") ,new FloatWritable(tripPassenger/totalNumberTrip));
				}

			
	 }
}