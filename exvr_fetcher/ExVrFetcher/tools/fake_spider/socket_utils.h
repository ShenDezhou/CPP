#ifndef BK_SOKCET_UTILS_H
#define BK_SOCKET_UTILS_H
#include "fcntl.h"
#include "string"
#include "errno.h"
#include <sys/types.h> 
#include <string.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <errno.h> 
#include <sys/wait.h> 
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include "string_Util.h"
#include <netinet/in.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include "string.h"
#include "stdio.h"
#include <Platform/encoding.h>
#include <Platform/log.h>
#include <time.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <net/if.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define INVALID_FD                  (-1)
#define MAX_FD_NUM                  1024

#define SOCKET_SND_BUF_SIZE         (1024*1024)
#define SOCKET_RCV_BUF_SIZE         (1024*1024)
#define MAX_REQUEST_SIZE            (1024*4)
#define MIN_RECEIVE_SIZE            1000
#define MIN_REPLY_SIZE              1000
#define RECEIVE_TIMEOUT             100
#define REPLY_TIMEOUT               100

#define SERVER_SOCKET               0x00000001U
#define MANAGER_SOCKET              0x00000002U
#define SYSTEM_SOCKET               0x00000004U
#define SYSTEM_PIPE                 0x00000008U
#define READABLE_SOCKET             0x00000010U
#define WRITABLE_SOCKET             0x00000020U

void 	SleepMsec ( int iMsec );
int 	create_pipe(int &_pipe_read,int &_pipe_write);

int     get_fd_info(int fd, char* ipaddr,int strLen);
int 	asyn_connect_timeout(struct sockaddr_in *_addr,timeval *_conn_timeout);
int 	set_socket(int fd, int flag);
int 	writen_timeout(int fd, const void *buf, size_t buf_len, timeval *timeout);
int 	read_once_timeout(int _fd, void *buf, size_t buf_len, timeval *timeout);
int 	readn_timeout(int fd, void *buf, size_t buf_len, timeval *timeout);

int 	getHostofUrl(const std::string &_url,int &retBeg,int &retEnd);
int 	get_host_port(const std::string &strAddr,std::string &_host,int &port);
int 	gethostAddr(const std::string hostInfo,struct sockaddr_in &sockAddr);
int		gethostAddr(const std::string _ip,const int port, struct sockaddr_in &sockAddr);
int     create_server_socket(int &socket_fd,int port,int listenNum);
int 	gettimeLen_us(timeval &_end,timeval &_beg);
std::string	GetLocalIp();
std::string hostnameToIp(std::string name);
bool 	sockadd_in_eq(struct sockaddr_in &sock_a,struct sockaddr_in &sock_b);
#endif
