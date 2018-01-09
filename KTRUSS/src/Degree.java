
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class KTRUSS {
	//k-truss��������
	static int k=3;
	
	//***
	//����1: ���㶥��Ķ�
	//***
	public static class Map_First extends Mapper<Object,Text,Text,Text>
	{
		Text edge=new Text();
		private FileSplit split;
		public void map(Object key, Text value, Context context)
		throws IOException,InterruptedException
		{
			split=(FileSplit)context.getInputSplit();
			
			String v1,v2;//�����ڵ�������
			
			//���ո�ָ�
			String txt=value.toString();
			StringTokenizer itr=new StringTokenizer(txt);
			while(itr.hasMoreTokens())
			{
				v1=new String(itr.nextToken());
				v2=new String(itr.nextToken());
				
				//����С�ķ�ǰ��
				if(v1.compareTo(v2)<=0)
				{
					context.write(new Text(v1),new Text(v1+"#"+v2));
				}
				else
				{
					context.write(new Text(v1),new Text(v2+"#"+v1));
				}
			//	context.write(new Text(v2),new Text(v1+"#"+v2));
			}
		}
	}
	public static class Reduce_First extends Reducer<Text,Text,Text,Text>
	{
		Text vector=new Text();//����
		ArrayList<Text> edgeList;//�����Ӧ�ı߼���
		int degree;//����Ķ���
		Text result=new Text();//���ÿһ��key��Ӧ���ĵ�
		public void reduce(Text key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException
		{
			//��ʼ��
			edgeList=new ArrayList<Text>();
			vector.set(key);
			
			int count=0;//�ýڵ�Ķ���
			for(Text value:values)
			{
				count++;
				edgeList.add(new Text(value));
			}
			
			degree=count;
			
			for(Text value:edgeList)
			{
				String []vectors=value.toString().split("#");
				
				if(vectors[0].equals(vector.toString()))
					context.write(value,new Text(value.toString()+":"+degree+":"+0));
				if(vectors[1].equals(vector.toString()))
					context.write(value,new Text(value.toString()+":"+0+":"+degree));
			}
		}
	}
	public static class Map_Second extends Mapper<Object,Text,Text,Text>
	{
		Text oKey=new Text();
		Text oValue=new Text();
		public void map(Object key, Text value, Context context)
		throws IOException,InterruptedException
		{
	//		context.write((Text)key,value);
			
			//���ո�ָ�
			String txt=value.toString();
			StringTokenizer itr=new StringTokenizer(txt);
			while(itr.hasMoreTokens())
			{
				oKey.set(itr.nextToken());
				oValue.set(itr.nextToken());
				context.write(oKey, oValue);
			}
		}
	}
	public static class Reduce_Second extends Reducer<Text,Text,Text,Text>
	{
		Text Degree1=new Text();
		Text Degree2=new Text();
		public void reduce(Text key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException
		{
			for(Text value:values)
			{
				String []str=value.toString().split(":");
				if(Integer.parseInt(str[1])>0)//****degree1,degree2 set����
				{
					Degree1.set(str[1]);
				}
				else
				{
					Degree2.set(str[2]);
				}
			}
			
			context.write(key,new Text(key.toString()+":"+Degree1.toString()+":"+Degree2.toString()));
		}
	}
	//***
	//����2 : ö��������
	//***
	public static class Map_Third extends Mapper<Object,Text,Text,Text>
	{
		Text minV=new Text();
		Text edge=new Text();
		public void map(Object key, Text value, Context context)
		throws IOException,InterruptedException
		{			
			//���ո�ָ�
			String txt=value.toString();
			StringTokenizer itr=new StringTokenizer(txt);
			
			String v1,v2;//���Ҷ���
			int degree1,degree2;//���Ҷ���
			while(itr.hasMoreTokens())
			{
				//ȡ����
				String vertexs[]=new String(itr.nextToken()).split("#");
				v1=vertexs[0];
				v2=vertexs[1];
				
				//ȡ�ߺͶ���
				String row=new String(itr.nextToken());
				String part[]=row.split(":");
				
				edge.set(part[0]);
				degree1=Integer.valueOf(part[1]);
				degree2=Integer.valueOf(part[2]);
				
				//�Ƚϴ�С
				if(degree1>1&&degree2>1)
				{
					if(degree1<degree2)
					{
						context.write(new Text(v1), edge);
					}
					else if(degree1>=degree2)
					{
						context.write(new Text(v2), edge);
					}
				}
			}
		}
	}
	public static class Reduce_Third extends Reducer<Text,Text,Text,Text>
	{
		Text oKey=new Text();
		Text oValue=new Text();
		ArrayList<Text> edgeList;
		public void reduce(Text key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException
		{
			edgeList=new ArrayList<Text>();
			
			int count=0;
			for(Text value:values)
			{
				edgeList.add(new Text(value));
				count++;
				
				context.write(value, value);
			}
			
			if(count>1)
			{
				String v1,v2;//����key�������������
				
				//����������
				for(int edge1_i=0;edge1_i<edgeList.size();edge1_i++)
				{
					for(int edge2_i=edge1_i+1;edge2_i<edgeList.size();edge2_i++)
					{
						//ȡ��
						String edge1=new String(edgeList.get(edge1_i).toString());
						String edge2=new String(edgeList.get(edge2_i).toString());
						
						//ȡ������key�����������������
						String edge1_vertexs[]=edge1.split("#");
						String edge2_vertexs[]=edge2.split("#");
						
						if(key.toString().equals(edge1_vertexs[0]))
						{
							v1=new String(edge1_vertexs[1]);
						}
						else
						{
							v1=new String(edge1_vertexs[0]);
						}
						
						if(key.toString().equals(edge2_vertexs[0]))
						{
							v2=new String(edge2_vertexs[1]);
						}
						else
						{
							v2=new String(edge2_vertexs[0]);
						}
						
						//�����ִ�С��ɱ߷���
						if(v1.compareTo(v2)<=0)
						{
							context.write(new Text(v1+"#"+v2), new Text(edge1+":"+edge2));
						}
						else
						{
							context.write(new Text(v2+"#"+v1), new Text(edge2+":"+edge1));
						}
					}
				}
			
			}
	
		}
	}
	public static class Map_Forth extends Mapper<Object,Text,Text,Text>
	{
		Text oKey=new Text();
		Text oValue=new Text();
		public void map(Object key, Text value, Context context)
		throws IOException,InterruptedException
		{			
			//���ո�ָ�
			String txt=value.toString();
			StringTokenizer itr=new StringTokenizer(txt);
			
			while(itr.hasMoreTokens())
			{
				oKey.set(itr.nextToken());
				oValue.set(itr.nextToken());
				
				context.write(oKey, oValue);
			}
		}
	}
	public static class Reduce_Forth extends Reducer<Text,Text,Text,Text>
	{
		Text oKey=new Text();
		Text oValue=new Text();
		ArrayList<Text> edgeList;
		public void reduce(Text key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException
		{
			edgeList=new ArrayList<Text>();
			
			boolean isFind=false;//�Ƿ��ҵ�������
			
			int count=0;
			for(Text value:values)
			{
				if(!key.toString().equals(value.toString()))
				{
					edgeList.add(new Text(value));
				}
				else
				{
					isFind=true;
				}
				count++;
			}			
			
			if(count>1&&isFind)
			{
				for(Text edge:edgeList)
				{
					context.write(key, new Text(key.toString()+":"+edge.toString()));
				}
				
		//		String edge1=edgeList.get(0).toString();
		//		String edge2=edgeList.get(1).toString();
				
		//		context.write(key, new Text(edge1+":"+edge2));
			}
	
		}
	}
	//***
	//����3 : ͳ������k-truss������
	//***
	public static class Map_Fifth extends Mapper<Object,Text,Text,Text>
	{
		private FileSplit split;
		public void map(Object key, Text value, Context context)
		throws IOException,InterruptedException
		{
			split=(FileSplit)context.getInputSplit();
			
			//���ո�ָ�
			String txt=value.toString();
			StringTokenizer itr=new StringTokenizer(txt);
			while(itr.hasMoreTokens())
			{
				itr.nextToken();
				
				//ȡ�������εĸ���
				String row=new String(itr.nextToken());
				String []edge=row.split(":");
				
				context.write(new Text(edge[0]), new Text("1"));
				context.write(new Text(edge[1]), new Text("1"));
				context.write(new Text(edge[2]), new Text("1"));
			}
		}
	}
	public static class Reduce_Fifth extends Reducer<Text,Text,Text,Text>
	{
		Text result=new Text();//���ÿһ��key��Ӧ���ĵ�
		public enum UpdateCounter {
			  UPDATED
			 }
		public void reduce(Text key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException
		{
			int count=0;
			for(Text value:values)
			{
				count+=Integer.valueOf(value.toString());
			}
			
			if(count>=k-2)//����k-truss��������ôȥ����
			{
				//����counter����
				String []vertexs=key.toString().split("#");
				context.write(new Text(vertexs[0]), new Text(vertexs[1]));
				context.write(new Text(vertexs[1]), new Text(vertexs[0]));
			}
			else//�����㲻����ߣ�ͬʱ��ʾ���ڸ���
			{
				context.getCounter(UpdateCounter.UPDATED).increment(1);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

       // ��仰�ܹؼ�
		
       conf.set("mapred.job.tracker", "192.168.206.21:9001");
       
       conf.set("mapred.jar","C:/Users/Jason/Desktop/����γ�/������ѧ��/�����㷨/JARS/KTRUSS.jar");

       //�����������·��
       String[] ioArgs = new String[] { "hdfs://192.168.206.21:9000/user/KTRUSS/0/input", "hdfs://192.168.206.21:9000/user/KTRUSS/0/temp1","hdfs://192.168.206.21:9000/user/KTRUSS/0/temp2","hdfs://192.168.206.21:9000/user/KTRUSS/0/temp3","hdfs://192.168.206.21:9000/user/KTRUSS/0/temp4","hdfs://192.168.206.21:9000/user/KTRUSS/0/output" };

       String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

       if (otherArgs.length != 6) {

    	   System.err.println("Usage: Inverted Index <in> <out>");

    	   System.exit(2);

       }
       
       FileSystem fs = FileSystem.get(conf);
    	   
    	   
       Path in,out;
       int iterNum=1;
       boolean hasUpdates=true;

       while(hasUpdates)
       {
    	   
    	   
    	   
       //=================================����Job1:Map��Combine��Reduce����==============================================

       Job job1 = new Job(conf, "Job1");

       job1.setJarByClass(KTRUSS.class);

       job1.setMapperClass(Map_First.class);

 //      job.setCombinerClass(Combine.class);

       job1.setReducerClass(Reduce_First.class);
       
       // �����������

       job1.setOutputKeyClass(Text.class);
     
 //      job.setMapOutputKeyClass(TextArrayWritable.class);
 //      job.setMapOutputValueClass(IntWritable.class);
       job1.setOutputValueClass(Text.class);

       // ����������ݼ��ָ��С���ݿ�splites���ṩһ��RecordReder��ʵ��

       job1.setInputFormatClass(TextInputFormat.class);

       // �ṩһ��RecordWriter��ʵ�֣������������

       job1.setOutputFormatClass(TextOutputFormat.class);

       //������е����Ŀ¼
       in=new Path(otherArgs[0]);
       out=new Path(otherArgs[1]);
       
       if(fs.exists(out))
       {
    	   fs.delete(out, true);
       }
       
       // ������������Ŀ¼
       FileInputFormat.addInputPath(job1, in);

       FileOutputFormat.setOutputPath(job1, out);
       
       ControlledJob ctrljob1=new  ControlledJob(conf); 
       ctrljob1.setJob(job1);
       
       //=================================����Job2:Map��Combine��Reduce����==============================================

       Job job2 = new Job(conf, "Job2");

       job2.setJarByClass(KTRUSS.class);

       job2.setMapperClass(Map_Second.class);

 //      job.setCombinerClass(Combine.class);

       job2.setReducerClass(Reduce_Second.class);
       
       // �����������
       job2.setOutputKeyClass(Text.class);
       job2.setOutputValueClass(Text.class);
 //      job.setMapOutputKeyClass(TextArrayWritable.class);
 //      job.setMapOutputValueClass(IntWritable.class);

       // ����������ݼ��ָ��С���ݿ�splites���ṩһ��RecordReder��ʵ��
       job2.setInputFormatClass(TextInputFormat.class);

       // �ṩһ��RecordWriter��ʵ�֣������������

       job2.setOutputFormatClass(TextOutputFormat.class);
       
       //������е����Ŀ¼
       in=new Path(otherArgs[1]);
       out=new Path(otherArgs[2]);
       
       if(fs.exists(out))
       {
    	   fs.delete(out, true);
       }

       // ������������Ŀ¼
       FileInputFormat.addInputPath(job2, in);

       FileOutputFormat.setOutputPath(job2, out);
       
       ControlledJob ctrljob2=new  ControlledJob(conf);
       ctrljob2.setJob(job2);
       
       //���������
       ctrljob2.addDependingJob(ctrljob1);
       
       
     //=================================����Job3:Map��Combine��Reduce����==============================================

       Job job3 = new Job(conf, "Job3");

       job3.setJarByClass(KTRUSS.class);

       job3.setMapperClass(Map_Third.class);

 //      job.setCombinerClass(Combine.class);

       job3.setReducerClass(Reduce_Third.class);
       
       // �����������
       job3.setOutputKeyClass(Text.class);
       job3.setOutputValueClass(Text.class);
 //      job.setMapOutputKeyClass(TextArrayWritable.class);
 //      job.setMapOutputValueClass(IntWritable.class);

       // ����������ݼ��ָ��С���ݿ�splites���ṩһ��RecordReder��ʵ��
       job3.setInputFormatClass(TextInputFormat.class);

       // �ṩһ��RecordWriter��ʵ�֣������������

       job3.setOutputFormatClass(TextOutputFormat.class);

       //������е����Ŀ¼
       in=new Path(otherArgs[2]);
       out=new Path(otherArgs[3]);
       
       if(fs.exists(out))
       {
    	   fs.delete(out, true);
       }
       
       // ������������Ŀ¼
       FileInputFormat.addInputPath(job3, in);

       FileOutputFormat.setOutputPath(job3, out);
       
       ControlledJob ctrljob3=new  ControlledJob(conf);
       ctrljob3.setJob(job3);
       
       //���������
       ctrljob3.addDependingJob(ctrljob2);
       
     //=================================����Job4:Map��Combine��Reduce����==============================================

       Job job4 = new Job(conf, "Job4");

       job4.setJarByClass(KTRUSS.class);

       job4.setMapperClass(Map_Forth.class);

 //      job.setCombinerClass(Combine.class);

       job4.setReducerClass(Reduce_Forth.class);
       
       // �����������
       job4.setOutputKeyClass(Text.class);
       job4.setOutputValueClass(Text.class);
 //      job.setMapOutputKeyClass(TextArrayWritable.class);
 //      job.setMapOutputValueClass(IntWritable.class);

       // ����������ݼ��ָ��С���ݿ�splites���ṩһ��RecordReder��ʵ��
       job4.setInputFormatClass(TextInputFormat.class);

       // �ṩһ��RecordWriter��ʵ�֣������������

       job4.setOutputFormatClass(TextOutputFormat.class);
       
       //������е����Ŀ¼
       in=new Path(otherArgs[3]);
       out=new Path(otherArgs[4]);
       
       if(fs.exists(out))
       {
    	   fs.delete(out, true);
       }

       // ������������Ŀ¼
       FileInputFormat.addInputPath(job4, in);

       FileOutputFormat.setOutputPath(job4, out);
       
       ControlledJob ctrljob4=new  ControlledJob(conf);
       ctrljob4.setJob(job4);
       
       //���������
       ctrljob4.addDependingJob(ctrljob3);
       
     //=================================����Job5:Map��Combine��Reduce����==============================================

       Job job5 = new Job(conf, "Job5");

       job5.setJarByClass(KTRUSS.class);

       job5.setMapperClass(Map_Fifth.class);

 //      job.setCombinerClass(Combine.class);

       job5.setReducerClass(Reduce_Fifth.class);
       
       // �����������
       job5.setOutputKeyClass(Text.class);
       job5.setOutputValueClass(Text.class);
 //      job.setMapOutputKeyClass(TextArrayWritable.class);
 //      job.setMapOutputValueClass(IntWritable.class);

       // ����������ݼ��ָ��С���ݿ�splites���ṩһ��RecordReder��ʵ��
       job5.setInputFormatClass(TextInputFormat.class);

       // �ṩһ��RecordWriter��ʵ�֣������������

       job5.setOutputFormatClass(TextOutputFormat.class);
       
     //������е����Ŀ¼
       in=new Path(otherArgs[4]);
       out=new Path(otherArgs[5]);
       
       if(fs.exists(out))
       {
    	   fs.delete(out, true);
       }

       // ������������Ŀ¼
       FileInputFormat.addInputPath(job5, in);

       FileOutputFormat.setOutputPath(job5, out);
       
       ControlledJob ctrljob5=new  ControlledJob(conf);
       ctrljob5.setJob(job5);
       
       //���������
       ctrljob5.addDependingJob(ctrljob4);
       
       //============================================����Job===============================================
       JobControl jc=new JobControl("123");
       
       jc.addJob(ctrljob1);
       jc.addJob(ctrljob2);
       jc.addJob(ctrljob3);
       jc.addJob(ctrljob4);
       jc.addJob(ctrljob5);

//       jc.run();
       
     //���߳���������סһ��Ҫ�����
       Thread  t=new Thread(jc); 
       t.start(); 

       while(true)
       { 
    	   if(jc.allFinished())
    	   {//�����ҵ�ɹ���ɣ��ʹ�ӡ�ɹ���ҵ����Ϣ 
    		   System.out.println(jc.getSuccessfulJobList()); 
    		   jc.stop(); 
    		   break; 
    	   }
    	   if(jc.getFailedJobList().size()>0){//�����ҵʧ�ܣ��ʹ�ӡʧ����ҵ����Ϣ 
    	   System.out.println(jc.getFailedJobList()); 

    	   jc.stop(); 
    	   break; 
    	   } 
       }
       
       
       //�����������·��
       ioArgs = new String[] { "hdfs://192.168.206.21:9000/user/KTRUSS/"+(iterNum-1)+"/output", "hdfs://192.168.206.21:9000/user/KTRUSS/"+iterNum+"/temp1","hdfs://192.168.206.21:9000/user/KTRUSS/"+iterNum+"/temp2","hdfs://192.168.206.21:9000/user/KTRUSS/"+iterNum+"/temp3","hdfs://192.168.206.21:9000/user/KTRUSS/"+iterNum+"/temp4","hdfs://192.168.206.21:9000/user/KTRUSS/"+iterNum+"/output" };

       otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
       
       iterNum++;
       
       job5.waitForCompletion(true);
       
       long counter = job5.getCounters().findCounter(Reduce_Fifth.UpdateCounter.UPDATED)
    		    .getValue();
       // compute termination condition
       hasUpdates = (counter > 0);
  
       }
       
 //      System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}