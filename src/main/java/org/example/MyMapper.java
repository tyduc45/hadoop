package org.example;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MyMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 解析CSV文件中的一行
        // 将 Text 转换为 String 并使用 split()
        String[] fieldStrings = value.toString().split(",");

        // 检查字段数量是否足够
        if (fieldStrings.length >= 17) {
            Double[] fields = new Double[fieldStrings.length];

            try {
                // 将 String[] 转换为 Double[]
                for (int i = 0; i < fieldStrings.length; i++) {
                    fields[i] = Double.parseDouble(fieldStrings[i]);
                }
            } catch (NumberFormatException e) {
                // 如果解析失败，跳过此记录或进行错误处理
                return; // 跳过此记录
            }

        Double timestamp = fields[0];          // 第一列作为时间戳
        Double velocity = fields[1];             // 第二列作为速度
        Double brake_output_l1 = fields[2];     // 第三到六列作为刹车输出力
        Double brake_output_l2 = fields[3];
        Double brake_output_r1 = fields[4];
        Double brake_output_r2 = fields[5];
        Double accx = fields[6];               // 第四列作为加速度
        Double engRPM = fields[7];           // 第五列作为发动机转速
        Double accy = fields[8];               // 第六列作为横向加速度
        Double strswr = fields[9];             // 第七列作为车辆转向角度
        Double qfuel = fields[10];             // 第八列作为燃油消
        Double cmps_r1 = fields[11];         // 第十一列作为右前轮弹簧压缩量
        Double cmps_r2 = fields[12];         // 第十二列作为右后轮弹簧压缩量
        Double cmps_l1 = fields[13];         // 第十三列作为左前轮弹簧压缩量
        Double cmps_l2 = fields[14];         // 第十四列作为左前轮弹簧压缩量

        Double[] brake = {brake_output_l1, brake_output_l2, brake_output_r1, brake_output_r2};

        // 第一组：Func1 -> (timestamp, velocity, brake[2])
        context.write(new Text("Func1"), new Text("TimeStamp: " + timestamp + ", Velocity: " + velocity + ", BrakeOutput: " + brake));

        // 第二组：Func 2 -> (timestamp, accx, engRPM, velocity)
        context.write(new Text("Func2"), new Text("TimeStamp: " + timestamp + " Accx: " + accx + "EngRPM: " + engRPM +  "Velocity" + velocity));

        // 第三组：Func3 -> (timestamp, strswr, accy)
        context.write(new Text("Func3"), new Text("TimeStamp: " + timestamp + ", StrSWr: " + strswr + ", Accy: " + accy));

        // 第四组：Func4 -> (cmps_r1, cmps_r2, cmps_l1, cmps_l2)
        context.write(new Text("Func4"), new Text("CmpS_r1: " + cmps_r1 + "CmpS_r2: " + cmps_r2 + "CmpS_l1: " + cmps_l1 + "CmpS_l2: " + cmps_l2));

        // 第五组：Func5 -> (timestamp, engRPM, velocity, qfuel)
        context.write(new Text("Func5"), new Text("TimeStamp: " + timestamp + ", EngRPM: " + engRPM + ", Velocity: " + velocity + ", Qfuel: " + qfuel));

        // 第六组：Func6 -> (timestamp, velocity, accy, strswr)
        context.write(new Text("Func6"), new Text("TimeStamp: " + timestamp + ", velocity: " + velocity + ", accy: " + accy + ", StrSWr: " + strswr));
    }
}
}
