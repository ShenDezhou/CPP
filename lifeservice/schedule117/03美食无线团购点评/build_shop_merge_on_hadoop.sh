#shopInput="data/nuomi_dianping_shops"

shopInput=$1;  shopMergeOutput=$2

hadoop dfs -rm /user/web_tupu/serviceapp/nuomi/merge/input/$(basename $shopInput)
hadoop dfs -put $shopInput /user/web_tupu/serviceapp/nuomi/merge/input/
echo "upload $shopInput into hdfs done."

#hadoop dfs -rmr /user/web_tupu/serviceapp/nuomi/merge/output
hadoop dfs -rmr $shopMergeOutput
hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.reduce.tasks=30 \
	-input /user/web_tupu/serviceapp/nuomi/merge/input/$(basename $shopInput) \
	-output $shopMergeOutput \
	-mapper ShopMergerMapper.py \
	-reducer ShopMergerReducer.py \
	-file bin/ShopMergerMapper.py \
	-file bin/ShopMergerReducer.py \
	-jobconf mapred.max.map.failures.percent="5" \
	-numReduceTasks 50	





