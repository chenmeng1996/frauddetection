package simple;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * element in control stream should be filtered out in another data stream
 * Note: because there is no order between two streams, some element may not be filtered
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env.fromElements("HAHA", "DROP", "IGNORE")
                .keyBy(x-> x);
        DataStream<String> streamOfWords = env.fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        control.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) throws Exception {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String controlValue, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(dataValue);
            }
        }
    }
}
