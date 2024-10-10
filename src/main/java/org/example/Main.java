package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        // 检查输入输出路径参数
        if (args.length != 2) {
            System.err.println("Usage: MainClass <input path> <output path>");
            System.exit(-1);
        }

        // 创建新的Job，并指定当前类为主类
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CSV Processing Job");
        job.setJarByClass(Main.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置Mapper输出键和值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终输出键和值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业并等待作业完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}