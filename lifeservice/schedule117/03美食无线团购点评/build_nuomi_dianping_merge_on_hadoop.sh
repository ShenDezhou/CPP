shopInput="data/nuomi_dianping_shops"

hadoop dfs -rm /user/web_tupu/serviceapp/nuomi/merge/input/*
hadoop dfs -put $shopInput /user/web_tupu/serviceapp/nuomi/merge/input/
echo "upload $shopInput into hdfs done."

hadoop dfs -rmr /user/web_tupu/serviceapp/nuomi/merge/output
hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.reduce.tasks=30 \
	-input /user/web_tupu/serviceapp/nuomi/merge/input/nuomi_dianping_shops \
	-output /user/web_tupu/serviceapp/nuomi/merge/output/ \
	-mapper ShopMergerMapper.py \
	-reducer ShopMergerReducer.py \
	-file bin/ShopMergerMapper.py \
	-file bin/ShopMergerReducer.py \
	-jobconf mapred.max.map.failures.percent="5" \
	-numReduceTasks 50	





