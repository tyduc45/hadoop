package org.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String group = key.toString();
        if(group.equals("Func1")) {
            processFunc1(values, context);
        } else if(group.equals("Func2")) {
            processFunc2(values, context);
        } else if(group.equals("Func3")) {
            processFunc3(values, context);
        } else if(group.equals("Func4")) {
            processFunc4(values, context);
        } else if(group.equals("Func5")) {
            processFunc5(values, context);
        } else if(group.equals("Func6")) {
            processFunc6(values, context);
        }
    }

    // 定义Func1的数据类
    private static class Func1Data {
        Double timestamp;
        Double velocity;
        Double[] brake;
        public Func1Data(Double timestamp, Double velocity, Double[] brake) {
            this.timestamp = timestamp;
            this.velocity = velocity;
            this.brake = brake;
        }
    }

    // Func1：制动效率分析
    private void processFunc1(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 存储数据并按时间戳排序
        List<Func1Data> dataList = new ArrayList<>();
        for(Text value : values) {
            String[] fields = value.toString().split(",");
            if(fields.length >= 6) {
                Double timestamp = Double.parseDouble(fields[0]);
                Double velocity = Double.parseDouble(fields[1]);
                Double brake_output_l1 = Double.parseDouble(fields[2]);
                Double brake_output_l2 = Double.parseDouble(fields[3]);
                Double brake_output_r1 = Double.parseDouble(fields[4]);
                Double brake_output_r2 = Double.parseDouble(fields[5]);
                dataList.add(new Func1Data(timestamp, velocity, new Double[]{brake_output_l1, brake_output_l2, brake_output_r1, brake_output_r2}));
            }
        }
        // 按时间戳排序
        Collections.sort(dataList, Comparator.comparingDouble(d -> d.timestamp));

        // 对每个刹车进行分析
        String[] brakeNames = {"brake_output_l1", "brake_output_l2", "brake_output_r1", "brake_output_r2"};
        for(int brakeIndex = 0; brakeIndex < 4; brakeIndex++) {
            List<Func1Data> brakeDataList = new ArrayList<>();
            // 提取当前刹车的数据
            for(Func1Data data : dataList) {
                Double brakeValue = data.brake[brakeIndex];
                brakeDataList.add(new Func1Data(data.timestamp, data.velocity, new Double[]{brakeValue}));
            }
            // 处理连续非零刹车段
            boolean inNonZeroSequence = false;
            List<Func1Data> sequenceData = new ArrayList<>();
            for(int i = 0; i < brakeDataList.size(); i++) {
                Func1Data data = brakeDataList.get(i);
                Double brakeValue = data.brake[0];
                if(brakeValue != 0) {
                    if(!inNonZeroSequence) {
                        inNonZeroSequence = true;
                        sequenceData.clear();
                        sequenceData.add(data);
                    } else {
                        sequenceData.add(data);
                    }
                } else {
                    if(inNonZeroSequence) {
                        inNonZeroSequence = false;
                        computeEfficiencyFactor(sequenceData, brakeNames[brakeIndex], context);
                    }
                }
            }
            // 如果最后一段是非零序列
            if(inNonZeroSequence) {
                computeEfficiencyFactor(sequenceData, brakeNames[brakeIndex], context);
            }
        }
    }

    private void computeEfficiencyFactor(List<Func1Data> sequenceData, String brakeName, Context context) throws IOException, InterruptedException {
        for(int i = 1; i < sequenceData.size(); i++) {
            Func1Data currentData = sequenceData.get(i);
            Func1Data previousData = sequenceData.get(i-1);
            Double initialSpeed = previousData.velocity;
            Double finalSpeed = currentData.velocity;
            Double brakeValue = currentData.brake[0];
            if(brakeValue != 0) {
                Double efficiencyFactor = (initialSpeed - finalSpeed) / brakeValue;
                context.write(new Text("Func1"), new Text("Timestamp: " + currentData.timestamp + ", Brake: " + brakeName + ", EfficiencyFactor: " + efficiencyFactor));
            }
        }
    }

    // Func2：动力系统健康预测
    private void processFunc2(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values) {
            String[] fields = value.toString().split(",");
            if(fields.length >= 4) {
                Double timestamp = Double.parseDouble(fields[0]);
                Double accx = Double.parseDouble(fields[1]);
                Double engRPM = Double.parseDouble(fields[2]);
                Double velocity = Double.parseDouble(fields[3]);
                if(velocity != 0 && accx != 0) {
                    Double powerFactor = engRPM / (velocity * accx);
                    context.write(new Text("Func2"), new Text("Timestamp: " + timestamp + ", PowerFactor: " + powerFactor));
                }
            }
        }
    }

    // Func3：轮胎侧偏角
    private void processFunc3(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values) {
            String[] fields = value.toString().split(",");
            if(fields.length >= 4) {
                Double timestamp = Double.parseDouble(fields[0]);
                Double velocity = Double.parseDouble(fields[1]);
                Double strswr = Double.parseDouble(fields[2]);
                Double accy = Double.parseDouble(fields[3]);
                if(velocity != 0) {
                    Double slipAngle = Math.atan(accy / velocity) - strswr;
                    context.write(new Text("Func3"), new Text("Timestamp: " + timestamp + ", SlipAngle: " + slipAngle));
                }
            }
        }
    }

    // Func4：车辆稳定性分析
    private void processFunc4(Iterable<Text> values, Context context)  throws IOException, InterruptedException{
        ArrayList<Double> cmps_r1_values = new ArrayList<>();
        ArrayList<Double> cmps_r2_values = new ArrayList<>();
        ArrayList<Double> cmps_l1_values = new ArrayList<>();
        ArrayList<Double> cmps_l2_values = new ArrayList<>();

        // 读取数据
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            if (fields.length >= 4) {
                cmps_r1_values.add(Double.parseDouble(fields[0]));
                cmps_r2_values.add(Double.parseDouble(fields[1]));
                cmps_l1_values.add(Double.parseDouble(fields[2]));
                cmps_l2_values.add(Double.parseDouble(fields[3]));
            }
        }
        // 计算方差
        double variance_r1 = calculateVariance(cmps_r1_values);
        double variance_r2 = calculateVariance(cmps_r2_values);
        double variance_l1 = calculateVariance(cmps_l1_values);
        double variance_l2 = calculateVariance(cmps_l2_values);

        // 计算均值
        double meanVariance = (variance_r1 + variance_r2 + variance_l1 + variance_l2) / 4;

        // 计算差异
        double diff_r1 = variance_r1 - meanVariance;
        double diff_r2 = variance_r2 - meanVariance;
        double diff_l1 = variance_l1 - meanVariance;
        double diff_l2 = variance_l2 - meanVariance;

        // 输出结果
        context.write(new Text("Func4"),
                new Text("Average Variance: " + meanVariance +
                        ", Diff r1: " + diff_r1 +
                        ", Diff r2: " + diff_r2 +
                        ", Diff l1: " + diff_l1 +
                        ", Diff l2: " + diff_l2));
    }
    // 方差计算方法
    private double calculateVariance(ArrayList<Double> values) {
        double sum = 0;
        for (Double val : values) {
            sum += val;
        }
        double mean = sum / values.size();
        double variance = 0;
        for (Double val : values) {
            variance += (val - mean) * (val - mean);
        }
        return variance / values.size();
    }

    // Func5：能耗效率分析
    private void processFunc5(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values) {
            String[] fields = value.toString().split(",");
            if(fields.length >= 4) {
                Double timestamp = Double.parseDouble(fields[0]);
                Double engRPM = Double.parseDouble(fields[1]);
                Double velocity = Double.parseDouble(fields[2]);
                Double qfuel = Double.parseDouble(fields[3]);
                if(engRPM != 0 && velocity != 0) {
                    Double efficiencyRatio = qfuel / (engRPM * velocity);
                    context.write(new Text("Func5"), new Text("Timestamp: " + timestamp + ", EfficiencyRatio: " + efficiencyRatio));
                }
            }
        }
    }

    // Func6：操纵性评估
    private void processFunc6(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values) {
            String[] fields = value.toString().split(",");
            if(fields.length >= 4) {
                Double timestamp = Double.parseDouble(fields[0]);
                Double velocity = Double.parseDouble(fields[1]);
                Double accy = Double.parseDouble(fields[2]);
                Double strswr = Double.parseDouble(fields[3]);
                if(velocity != 0 && accy != 0) {
                    Double responsivenessIndex = strswr / (velocity * accy);
                    context.write(new Text("Func6"), new Text("Timestamp: " + timestamp + ", ResponsivenessIndex: " + responsivenessIndex));
                }
            }
        }
    }
}

