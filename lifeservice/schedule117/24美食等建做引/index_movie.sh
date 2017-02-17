#!/bin/bash

. ./Tool/Shell/Tool.sh


cmRelLine=$(cat /fuwu/Merger/Output/movie/cinema_movie_rel.table | wc -l)
movieLine=$(cat /fuwu/Merger/Output/movie/movie_detail.table | wc -l)

[ $cmRelLine -lt 10000 ] && echo "cinema-movie-relation file is too small" && exit -1
[ $movieLine -lt 80 ] && echo "movie file is too small" && exit -1


ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_movie.sh 1>std 2>err &"

echo "done."
