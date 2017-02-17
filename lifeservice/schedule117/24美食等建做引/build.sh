echo `date "+%Y-%m-%d %H:%M"`
#Source
cd Source/
sh -x build.sh 1>std 2>err
cd -

echo `date "+%Y-%m-%d %H:%M"`
#Merger
cd Merger/
sh -x build.sh 1>std 2>err
cd -

echo `date "+%Y-%m-%d %H:%M"`
#Tables
cd Tables/
sh -x build.sh 1>std 2>err
cd -

echo `date "+%Y-%m-%d %H:%M"`
#Index
cd Index/
sh -x build.sh 1>std 2>err
cd -
