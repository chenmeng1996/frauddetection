/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	// record whether previous transaction was small
	private transient ValueState<Boolean> flagState;

	// record next tigger time of timer
	private transient ValueState<Long> timerState;


	@Override
	public void processElement(
			Transaction transaction, // each element in data stream
			Context context,
			Collector<Alert> collector // collect Alert element, send to downstream
	) throws Exception {

		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();

		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				// if a small transaction is followed by a large one, output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}

			cleanUp(context);
		}

		// record a recent small transaction
		if (transaction.getAmount() < SMALL_AMOUNT) {
			flagState.update(true);

			// set the timer, trigger after 1 minute
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			// record the trigger time
			timerState.update(timer);
		}
	}

	// initialization method for this function.
	// in the method, initialize all state.
	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>("timer-state", Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	// method that timer trigger
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
		// remove flag after 1 minute
		timerState.clear();
		flagState.clear();
	}

	// clean all state, delete timer
	private void cleanUp(Context ctx) throws IOException {
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		timerState.clear();
		flagState.clear();
	}
}
