package simple;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * there is a stream of event to be de-duplicated, leaving only the first event for each key
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new TransactionSource())
                .keyBy(Transaction::getAccountId)
                .flatMap(new Deduplicator())
                .print();

        env.execute();
    }

    static class Deduplicator extends RichFlatMapFunction<Transaction, Transaction> {
        ValueState<Boolean> keyHasBeenSeen;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<Boolean>("keyHasBeenSeen", Types.BOOLEAN);
            keyHasBeenSeen = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(Transaction value, Collector<Transaction> out) throws Exception {
            if (keyHasBeenSeen.value() == null) {
                out.collect(value);
                keyHasBeenSeen.update(true);
            }
        }
    }

}
