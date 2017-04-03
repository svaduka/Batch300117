package com.laboros.mapper;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	final char dataDelimiter=',';

	int[] deIdentifyColumns={2,3,4,5,6,8,9};

	private byte[] key="abcdefgh12345678".getBytes();
	
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value -- 11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,1111111111,M,Diabetes,78
		
		final String inputLine=value.toString();
		StringBuffer finalOutput=new StringBuffer();
		if(!StringUtils.isEmpty(inputLine))
		{
			final String[] tokens=StringUtils.
								splitPreserveAllTokens(inputLine, dataDelimiter);
			
			for (int i = 0; i < tokens.length; i++) {
				
				//if condition identify whether i column is encrypt or not
				boolean isEncrypt=checkifEncrypt(i);
				if(isEncrypt)
				{
					String encryptedData;
					try {
						encryptedData = encrypt(tokens[i]);
						finalOutput.append(encryptedData);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (InvalidKeyException e) {
						e.printStackTrace();
					} catch (NoSuchPaddingException e) {
						e.printStackTrace();
					} catch (IllegalBlockSizeException e) {
						e.printStackTrace();
					} catch (BadPaddingException e) {
						e.printStackTrace();
					}
					
				}else{
					finalOutput.append(tokens[i]);
				}
				finalOutput.append(dataDelimiter);
			}
		}
		
		System.out.println(finalOutput);
		
		context.write(new Text(finalOutput.toString()), NullWritable.get());
		
		
	}

	private String encrypt(String column) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException 
	{
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		String encryptedString = Base64.encodeBase64String(cipher.doFinal(column.getBytes()));
		return encryptedString.trim();
	}

	private boolean checkifEncrypt(int iColIdx) {
		for (int i = 0; i < deIdentifyColumns.length; i++) {
			if((iColIdx+1)==deIdentifyColumns[i]){
				return Boolean.TRUE;
			}
		}
		return false;
	};
}
