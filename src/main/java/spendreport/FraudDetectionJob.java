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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		// 设置执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 数据源
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions");

		// 对数据源进行流操作
		DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId) // 按照账户id进行分区
			.process(new FraudDetector()) // 对流上的每个消息调用定义好的函数
			.name("fraud-detector");

		// 输出结果
		alerts
			.addSink(new AlertSink()) // INFO级别打印每个Alert的数据记录，而不是持久存储，方便查看结果
			.name("send-alerts");

		// 运行作业
		env.execute("Fraud Detection");
	}
}
