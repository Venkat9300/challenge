package com.ca.testUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Assignment extends UDF{

	String result = "";
  
	public Text evaluate(final Text s) {
		
	    if (s == null) { return null; }
	    
	    
	    String row = s.toString();
	    
	    
		String[] rowValue = row.split("#");
		
		//Date
		String rowDate = rowValue[0];
		
		if(row.split("#").length == 1) {
			return new Text(rowDate + ";null;null;null"); 
		}
		
		//Table heading impressions;conversions;clicks
		String[] type = rowValue[1].split(";");
		
		
		if((rowValue[1].split(";").length == 3) && (rowValue[2].split(";").length == 3)) {
			
			String[] valuesOfRow = rowValue[2].split(";");
			return new Text(rowDate + ";" + valuesOfRow[0] + ";" + valuesOfRow[2] + ";" + valuesOfRow[1] );
		}
		
		
		String values = rowValue[2];
		
		String[] rowValues = rowValue[2].split(";");
		
		
		if(rowValue[1].contains("impressions") && !rowValue[1].contains("conversions") && !rowValue[1].contains("clicks")) {
			values = values + ";null" + ";null";
		} 
		
		
		if(rowValue[1].contains("conversions") && !rowValue[1].contains("impressions") && !rowValue[1].contains("clicks")) {
			
			values = "null;"  + ";null" + rowValues[0];
		} 
		
		
		if(rowValue[1].contains("clicks") && !rowValue[1].contains("impressions") && !rowValue[1].contains("conversions")) {
			
			values = "null;" + rowValues[0] + "null;" ;
		}
		
		if(rowValue[1].contains("impressions") && rowValue[1].contains("conversions")) {
			String[] valuesOfRow = rowValue[2].split(";");
			
			values = valuesOfRow[0] + ";null;" + valuesOfRow[1] ;
		} 
		
		if(rowValue[1].contains("impressions") && rowValue[1].contains("clicks")) {
			
			values = rowValues[0]+ ";"  + rowValues[1] + ";null";
			
		} 
		
		if(rowValue[1].contains("conversions") && rowValue[1].contains("clicks")) {
			String[] valuesOfRow = rowValue[2].split(";");
			values = "null;" + valuesOfRow[1] + ";" +valuesOfRow[0];
		}
		result = rowDate + ";" + values;
	    return new Text(result);
  }
}
