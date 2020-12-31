package simple;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.time.Duration;

public class Example4 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Transaction> stream = env.addSource(new TransactionSource());

        WatermarkStrategy<Transaction> strategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20));
        DataStream<Transaction> withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(strategy);

        withTimestampsAndWatermarks.print();

        env.execute();
    }
}
