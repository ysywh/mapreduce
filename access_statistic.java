package access_statistic;  
  
import java.io.IOException;  
import java.util.StringTokenizer;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  

//about log ... 
public class access_statistic {  
    //继承mapper接口，设置map的输入类型为<Object,Text>,输出类型为<Text,IntWritable>  
    public static class Map extends Mapper<Object,Text,Text,IntWritable>{  
        //key_out作为输出键（”userID spName"）,value_out作为输出值 (上下总流量)
    	private static Text key_out = new Text(); 
        private static IntWritable value_out = new IntWritable(1);   
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{  
        	StringTokenizer st = new StringTokenizer(value.toString());  
        	String[] key_word = new String[8];
        	int i =0 ;
        	//把每一行切词，每行可以切出8个词，并存入数组key_word中
        	while(st.hasMoreTokens()){  
        		key_word[i++] = st.nextToken();
        	}
        	//判断是否为access.log第一行，当不是时候存入提取键（第3和第5个词）和值（第7和第8个词的值和）
        	if(! key_word[0].equals("time" )){
        		key_out.set(key_word[2]+"  "+key_word[5]);
        		value_out =  new IntWritable(Integer.parseInt(key_word[6]) + Integer.parseInt(key_word[7]));
        		context.write(key_out , value_out);   	
        	}          
        }  
    }  
    //继承reducer接口，设置reduce的输入类型<Text,IntWritable> 输出类型为<Text,Text>  
    public static class Reduce extends Reducer<Text,IntWritable,Text,Text>{  
        //result是reduce输出值（包含访问次数和总流量）
        private static Text result = new Text(); 
        private static int line = 1;  
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{ 
        	//为了在输出文件中加入第一行各个字段名字的说明
        	if(line++ == 1){
        		context.write(new Text("userID  spName"),new Text(" access_num  Traffic "));
        	}
        	//对获取的<key,value-list>计算value的和 为sum_flow，访问次数为sum_access
            int sum_flow = 0;
            int sum_access = 0;
            for(IntWritable val:values){  
                sum_access++;
                sum_flow += val.get();
            }  
            //将sum_flow，sum_access合并为sum设置到result 
            String sum = String.valueOf(sum_access) +"    " + String.valueOf(sum_flow);
            result.set(sum); 
            context.write(key, result);  
        } 
    }  
    /** 
     * @param args 
     */  
    public static void main(String[] args) throws Exception{  
        // TODO Auto-generated method stub  
        Configuration conf = new Configuration();  
        //配置作业名  
        Job job = new Job(conf,"acccess count");  
        //配置作业各个类  
        job.setJarByClass(access_statistic.class);  
        job.setMapperClass(Map.class);  
        job.setCombinerClass(Reduce.class);  
        job.setReducerClass(Reduce.class);  
        //由于map函数和reduce函数键值对类型不同，因此分别设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);

        //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        FileInputFormat.addInputPath(job, new Path("D:\\program\\hadoop\\Access_statistic1\\access.log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\program\\hadoop\\Access_statistic1\\access_out"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
  
}  