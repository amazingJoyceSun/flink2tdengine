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

package com.dtstack.portal;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.function.KafkaConsumer;
import com.dtstack.function.TDEngineSink;
import com.dtstack.util.KafkaSourceConfig;
import com.dtstack.util.TDEngineSinkConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Flink2TdEngine {

	private static final Logger logger = LoggerFactory.getLogger(Flink2TdEngine.class);

	public static void main(String[] args)  {
		logger.info("DataStreamJob init");
		ParameterTool rdfl =null;
		try {
			InputStream inputStream = Flink2TdEngine.class.getResourceAsStream("/application.properties");
			ParameterTool parameter = ParameterTool.fromPropertiesFile(inputStream);
			rdfl = parameter;
		}catch (IOException e){
			e.printStackTrace();
		}
		TDEngineSinkConfig tDenegineSinkConfig = new TDEngineSinkConfig(rdfl);

	/*  通过HDFS 文件路径读取配置文件
		用户觉得每次更新配置文件需要重新打包配加载配置文件比较麻烦，可以通过将配置文件上传到离线平台资源中的方式更新配置文件
		每次更新需要停止实时任务，重新运行
		hdfs文件路径通过程序运行入参指定，main方法的args以空格分割
		例如：hdfs://<namenode>:<port>/path/to/config.properties other-properties
		String hdfsPath = "hdfs://<namenode>:<port>/path/to/config.properties";
		hdfsPath = args[0];
		System.out.println("hdfs path ==>"+hdfsPath);
		try {
			// 通过 HadoopFileSystem 获取 HDFS 文件内容
			FileSystem fs = FileSystem.get(new Path(hdfsPath).toUri());
			//InputStreamReader reader = new InputStreamReader(fs.open(new Path(hdfsPath)));
			ParameterTool tl = ParameterTool.fromPropertiesFile(fs.open(new Path(hdfsPath)));
			rdfl = tl;
		}catch (IOException e){
			e.printStackTrace();
		}


	 */
		//开始初始化任务信息
		try {
			KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(rdfl);

			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			//调试可指定flink local运行模式
			//final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

			int parallelism = env.getParallelism();
			//重试策略
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
			//checkpoint间隔
			env.enableCheckpointing(60 * 1000 * 2, CheckpointingMode.AT_LEAST_ONCE);
//			env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);  //异常
//			env.setStateBackend(new HashMapStateBackend());  //异常
//			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);  //异常

			//构建kafka consumer source
			KafkaSource<String> kafka = KafkaConsumer.build(kafkaSourceConfig);
			//转换成流 指定source name、读取策略
			DataStreamSource<String> kafka_source = env
					.fromSource(kafka, WatermarkStrategy.noWatermarks(), "kafka-source");

			kafka_source.setParallelism(1)
					.flatMap(new MyFlatMap())
					.filter(new MyFilter())
					.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
					//	.trigger(trigger)
					.process(new MyProcessAllWindowFunction())
				.addSink(new TDEngineSink(tDenegineSinkConfig))
				.name("tdengine-sink");
			kafka_source.print().name("print_sink");

			env.execute("kafka-to_tdengine");
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	//数据转换函数，将读取的kafka数据在flatmap函数中进行转换处理（包括keyby、聚合、单条数据转换等），此处以单条数据转换为例
	public static class MyFlatMap implements FlatMapFunction<String, String>, Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String str, Collector<String> out) throws Exception {
			// TODO Auto-generated method stub
			JSONObject result = JSONObject.parseObject(str);
			result.put("flink-flat-time",System.currentTimeMillis());
			out.collect(result.toString());
			logger.info("flatMap kafka==>"+result.toString());
		}

	}
   
	public static class MyFilter implements FilterFunction<String>,Serializable{

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			// TODO Auto-generated method stub
			//过滤处理内容为空的数据
			return !StringUtils.isNullOrWhitespaceOnly(value);
		}
		
	}
	
	public static class MyProcessAllWindowFunction
			extends ProcessAllWindowFunction<String, String, TimeWindow> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 2391148942324090701L;

		@Override
		public void process(
				ProcessAllWindowFunction<String, String, TimeWindow>.Context ctx,
				Iterable<String> in, Collector<String> out) throws Exception {
//			InputStream inputStream = MyProcessAllWindowFunction.class.getResourceAsStream("/application.properties");
//			ParameterTool parameter = ParameterTool.fromPropertiesFile(inputStream);

			List<String> tmp = new ArrayList<String>();
			in.forEach(msg -> {
				tmp.add(msg);
				out.collect(msg);
			});
			logger.info("kafka process ==>"+tmp.toString());

		}
	}
	
}
