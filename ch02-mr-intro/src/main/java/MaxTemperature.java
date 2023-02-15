// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }

//    Job对象指定作业执行规范，可以用来控制整个作业的运行
    Job job = new Job();
//    在Hadoop集群上运行作业时，需要把代码打包成一个JAR文件，Hadoop在集群上发布这个文件，通过在Job对象的setJarByClass方法中传递一个类
//    Hadoop利用这个类来查找包含它的JAR文件
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max temperature");

//    指定输入和输出数据的路径
//    输入数据的路径可以是单个的文件、一个目录（此时将目录下的所有文件当作输入），或符合特定文件模式的一系列文件，可以多次调用实现多路径输入
    FileInputFormat.addInputPath(job, new Path(args[0]));
//    指定reduce函数输出文件的写入目录，在运行作业前，该目录不应该存在，否则Hadoop会报错并拒绝运行作业
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

//    指定需要使用的map类和reduce类
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);

//    控制reduce函数的输出类型，且必须和reduce类产生的相匹配，map函数的输出类型默认与reduce相同
//    如果不同，则需要通过setMapOutputKeyClass()和setMapOutputValueClass()方法设置map函数的输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

//    输入的类型通过setInputFormatClass()方法来控制，默认使用的是TextInputFormat(文本输入格式)
    job.setInputFormatClass(TextInputFormat.class);

//    waitForCompletion()方法负责提交作业并等待执行完成，唯一的参数是用来指示是否生成详细输出，为true时，作业会把其进度信息写到控制台
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
