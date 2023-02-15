// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
//  Mapper是一个泛型类型，四个形参类型，分别指定map函数的输入键、输入值、输出键和输出值的类型

  private static final int MISSING = 9999;
  
  @Override
//  LongWritable相当于Java的Long类型，Text相当于String类型，IntWritable相当于Integer类型
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

//    先转化为String类型
    String line = value.toString();
//    利用substring方法来提取需要的列数据
    String year = line.substring(15, 19);
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
//      context用于实例内容的写入
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}
// ^^ MaxTemperatureMapper
