
# hosts with NAT network config
NatHostList=(
	10.142.105.210
	10.143.9.182
	10.143.21.160
	10.143.22.226
	10.134.79.188
	10.134.79.154
	10.134.96.152
	10.142.104.204
	10.142.47.173
)


#10.142.105.210
NatHostList_bak=(
        10.142.104.204
        10.142.47.173
        10.143.9.182
        10.143.21.160
        10.143.22.226
        10.134.79.188
        10.139.26.144
        10.139.30.197
        10.139.27.217
        10.134.79.154
        10.134.96.152
)


# ok	10.142.105.210
# ok	10.142.104.204  # doing
# ok	10.142.47.173   # doing
# ok	10.143.9.182
# ok	10.143.21.160
# ok	10.143.22.226
# ok	10.134.79.188
# forbiden	10.139.26.144
# forbiden	10.139.30.197
# forbiden	10.139.27.217
# ok	10.134.79.154
# ok	10.134.96.152




# hosts with public ip 
publicHostList=(
	10.134.27.202
	10.134.50.168
	10.134.66.180
	10.134.77.134
	10.134.78.182
	10.134.86.145
	10.134.87.185
	10.134.91.212
	10.134.94.134
	10.134.95.220
	10.142.38.189
	10.142.61.159
	10.142.63.231
	10.142.70.230
	10.142.79.202
	10.142.86.132
	10.142.100.176
	10.142.101.149
	10.142.113.137
	10.142.116.170
)



# ≈˙¡ø÷¥––√¸¡Ó
function batch_command() {
	Path="/search/odin/Spider/"
	Script="sh bin/ping.sh"
	Passwd="Tupu@2015"

	#for host in ${NatHostList[@]}; do
	for host in ${NatHostList_bak[@]}; do
	#for host in ${publicHostList[@]}; do
		echo -e "==========  $host ============"
		#./expect/batch_command.exp $host $Passwd
		#./expect/execute_script.exp $host "$Path" "$Script" "$Passwd"
		#./expect/free_memory.exp $host $Passwd
		./expect/scp_dir.exp "/tmp/Getpage" $host":/search/odin/" "$Passwd"
		./expect/scp_file.exp "/tmp/parse-cheerio.js" $host":/search/odin/NodeParser/" "$Passwd"
		./expect/scp_file.exp "/tmp/common.js" $host":/search/odin/NodeParser/lib/" "$Passwd"
		./expect/scp_file.exp "/tmp/dianping_shop_parser.js" $host":/search/odin/NodeParser/Parser/" "$Passwd"
		

		echo -e "\n\n"
		sleep 2
	done
}


batch_command
