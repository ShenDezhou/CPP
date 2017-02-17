echo "begin to index foot service....."

ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_food.sh foot 1>tmp/foot.std 2>tmp/foot.err &"
