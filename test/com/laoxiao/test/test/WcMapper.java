package com.laoxiao.test.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class WcMapper extends Mapper<Object, Text, Text, IntWritable>{

	private static final IntWritable ONE = new IntWritable(1);
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//����ÿ��split��Ƭ
		String[] strs = StringUtils.split(value.toString(), ' ');
		
		//ÿ�����ʼ���һ��
		for(String s : strs){
			context.write(new Text(s), ONE);
		}
	}
}
