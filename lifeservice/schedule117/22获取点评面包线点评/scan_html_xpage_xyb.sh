#! /usr/bin/

BIN=./bin/
TMP=./tmp/
LOG=./log/

DBNETGET_BIN=${BIN}dbnetget
OFFSUM=conf/offsum576
PAGES=${TMP}$(basename $1)_htmlpages0
PAGESLOG=${LOG}$(basename $1)_htmlpages0.log

cat $1 | awk -F'\t' '{gsub("\r", "", $0);print $1}' | ${DBNETGET_BIN} -i url -o dd -d csum -df st -l ${OFFSUM} -pf ${PAGES} 1>${PAGESLOG} 2>&1
python ./bin/decode_parseJD_html_xpage.py ${PAGES} > ${PAGES}_res
