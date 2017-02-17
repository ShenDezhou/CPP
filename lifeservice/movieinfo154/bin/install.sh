sh install_chrome.sh
echo -e "\n\n =========== install chrome done.========"

sh install_vnc.sh
echo -e "\n\n =========== install vnc done.========"

cp vncservers /etc/sysconfig/vncservers

useradd vnc



