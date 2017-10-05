package q5_version;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Trip_Distance_Per_Day_Average_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	float WeekEndTripDist = 0;
	float WeekDayTripDist =0;
	float totalNumberTrip=0;
	float hrstripdistance=0;
	float hrsTripcnt=0;
	float wTripCnt=0;
	float wkdTripCnt=0;
	boolean f1=true;
	boolean f2=true;
	boolean f3=true;
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
	
		 		float sumVal = 0;
		 		if(key.find("TotalTripNumber")>0)
		 		{
		 			for (FloatWritable val : values)
					{
						sumVal+=val.get();
						
					}
		 			hrsTripcnt=sumVal;
		 		}
				if(key.find("TotalTripNumber")==-1)
				{
					
					for (FloatWritable val : values)
					{
						sumVal+=val.get();		
					}
					if(key.find("WeekEnd")==0 ||key.find("WorkingDay")==0)
						context.write(new Text(key+"_Avg"),new FloatWritable(sumVal/hrsTripcnt));
					else
						context.write(key,new FloatWritable(sumVal));
				}
				
				if(key.equals(new Text("Week End Trip Distance")))
				{		
					WeekEndTripDist=sumVal;
				}
				if(key.equals(new Text("Week End Trip Count")))
				{		
					wTripCnt=sumVal;
				}
				
				if(key.equals(new Text("Working Day Trip Distance")))
				{
					WeekDayTripDist=sumVal;
				}
				if(key.equals(new Text("Working Day Trip Count")))
				{
					wTripCnt=sumVal;
				}
				
				if(key.equals(new Text("Total Number of Trip")))
				{
					totalNumberTrip=sumVal;
				}
				if(f2 && WeekEndTripDist>0 && wTripCnt>0)
				{
					f2=false;
					context.write(new Text("Week End Average Trip Distance") ,new FloatWritable(WeekEndTripDist/wTripCnt));
				}
				if(f3 && WeekDayTripDist>0 && wkdTripCnt>0)
				{
					f3=false;
					context.write(new Text("Week Day Average Trip Distance") ,new FloatWritable(WeekDayTripDist/wkdTripCnt));
				}
				
				if(f1 && WeekEndTripDist>0 && WeekDayTripDist>0 &&totalNumberTrip>0)
				{
					f1=false;  //flag ensure that key is written once
					float totalTripDistance = WeekDayTripDist+WeekEndTripDist;
					context.write(new Text("Average Trip Distance") ,new FloatWritable(totalTripDistance/totalNumberTrip));
				}			
	 }
}