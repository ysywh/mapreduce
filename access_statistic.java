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
    //�̳�mapper�ӿڣ�����map����������Ϊ<Object,Text>,�������Ϊ<Text,IntWritable>  
    public static class Map extends Mapper<Object,Text,Text,IntWritable>{  
        //key_out��Ϊ���������userID spName"��,value_out��Ϊ���ֵ (����������)
    	private static Text key_out = new Text(); 
        private static IntWritable value_out = new IntWritable(1);   
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{  
        	StringTokenizer st = new StringTokenizer(value.toString());  
        	String[] key_word = new String[8];
        	int i =0 ;
        	//��ÿһ���дʣ�ÿ�п����г�8���ʣ�����������key_word��
        	while(st.hasMoreTokens()){  
        		key_word[i++] = st.nextToken();
        	}
        	//�ж��Ƿ�Ϊaccess.log��һ�У�������ʱ�������ȡ������3�͵�5���ʣ���ֵ����7�͵�8���ʵ�ֵ�ͣ�
        	if(! key_word[0].equals("time" )){
        		key_out.set(key_word[2]+"  "+key_word[5]);
        		value_out =  new IntWritable(Integer.parseInt(key_word[6]) + Integer.parseInt(key_word[7]));
        		context.write(key_out , value_out);   	
        	}          
        }  
    }  
    //�̳�reducer�ӿڣ�����reduce����������<Text,IntWritable> �������Ϊ<Text,Text>  
    public static class Reduce extends Reducer<Text,IntWritable,Text,Text>{  
        //result��reduce���ֵ���������ʴ�������������
        private static Text result = new Text(); 
        private static int line = 1;  
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{ 
        	//Ϊ��������ļ��м����һ�и����ֶ����ֵ�˵��
        	if(line++ == 1){
        		context.write(new Text("userID  spName"),new Text(" access_num  Traffic "));
        	}
        	//�Ի�ȡ��<key,value-list>����value�ĺ� Ϊsum_flow�����ʴ���Ϊsum_access
            int sum_flow = 0;
            int sum_access = 0;
            for(IntWritable val:values){  
                sum_access++;
                sum_flow += val.get();
            }  
            //��sum_flow��sum_access�ϲ�Ϊsum���õ�result 
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
        //������ҵ��  
        Job job = new Job(conf,"acccess count");  
        //������ҵ������  
        job.setJarByClass(access_statistic.class);  
        job.setMapperClass(Map.class);  
        job.setCombinerClass(Reduce.class);  
        job.setReducerClass(Reduce.class);  
        //����map������reduce������ֵ�����Ͳ�ͬ����˷ֱ�����
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