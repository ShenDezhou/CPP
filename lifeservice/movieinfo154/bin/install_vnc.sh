yum groupinstall Desktop -y
yum install gnome-core xfce4 -y
yum install tigervnc-server -y
chkconfig vncserver on

yum -y install fontforge
yum -y groupinstall "Chinese Support" 

