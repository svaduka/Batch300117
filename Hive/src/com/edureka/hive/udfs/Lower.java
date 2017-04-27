package com.edureka.hive.udfs;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class Lower extends UDF {
	
	public Text evaluate(final Text input)
	{
		final String inputStr=input.toString();
		Text output=new Text(inputStr.toLowerCase());
		return output;
	}

}
