#!/bin/sh
if [ $# -lt 4 ];then
    echo "usage: $0 conf dstdir username passwd"
    exit
fi
username=$3
passwd=$4
conf=$1
dstdir=$2
mkdir -p $dstdir
export SVNFLAGS="--username=$username --password=$passwd"
./docheckout.py $conf $dstdir
