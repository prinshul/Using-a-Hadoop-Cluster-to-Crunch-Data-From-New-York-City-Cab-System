package q4_version;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Passenger_perHour_by_Day_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	float tripPassWnkd = 0;
	float tripPassWkday =0;
	boolean f1=true;
	boolean f2=true;
	boolean f3=true;
	float tripCntHrs=0;
	float tripCntWeekends=0;
	float tripCntWeekday=0;

	float tCount=0;
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {

		float sumVal = 0;
		if(key.find("TotalTripNumber")>0)  // get correct key
		{
			for (FloatWritable val : values)
			{
				sumVal+=val.get();

			}
			tripCntHrs=sumVal;
		}
		if(key.find("TotalTripNumber")==-1)
		{

			for (FloatWritable val : values)
			{
				sumVal+=val.get();		
			}
			if(key.find("WeekEnd")==0 ||key.find("WorkingDay")==0)
			{
				context.write(new Text(key+"_Avg"),new FloatWritable(sumVal/tripCntHrs));
			}
			else
				context.write(key,new FloatWritable(sumVal));
		}

		if(key.equals(new Text("Week End Trip Passangers")))
		{		
			tripPassWnkd=sumVal;
		}
		if(key.equals(new Text("Week End Trip Count")))
		{		
			tripCntWeekends=sumVal;
		}

		if(key.equals(new Text("Working Day Trip Passangers")))
		{
			tripPassWkday=sumVal;
		}
		if(key.equals(new Text("Working Day Trip Count")))
		{
			tripCntWeekday=sumVal;
		}

		if(key.equals(new Text("Total Number of Trip")))
		{
			tCount=sumVal;
		}
		if(f2 && tripPassWnkd>0 && tripCntWeekends>0)
		{
			f2=false;
			context.write(new Text("Week End Average Trip Passanger") ,new FloatWritable(tripPassWnkd/tripCntWeekends));
		}
		if(f3 && tripPassWkday>0 && tripCntWeekday>0)
		{
			f3=false;  // flag is used to ensure that the key is written once
			context.write(new Text("Week Day Average Trip Passanger") ,new FloatWritable(tripPassWkday/tripCntWeekday));
		}
		if(f1 && tripPassWnkd>0 && tripPassWkday>0 &&tCount>0)
		{
			f1=false;
			float totalTripDistance = tripPassWkday+tripPassWnkd;
			context.write(new Text("Average Trip Passanger") ,new FloatWritable(totalTripDistance/tCount));
		}

	}
}