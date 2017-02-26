/*
Copyright (c) 2017-2018 Dezhou Shen, Sogou Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <string.h>
#include <signal.h>

#include <iconv.h>

#include "ace/OS_NS_string.h"
#include "ace/OS_NS_sys_stat.h"
#include "ace/Message_Block.h"
#include "ace/INET_Addr.h"
#include "ace/SOCK_Acceptor.h"
#include "ace/SOCK_Stream.h"
#include "ace/Auto_Ptr.h"

int s_iconv(const char* src, int size, char* result, ssize_t* r_size, char* in,char* out)
{
    int max_len = size*2+1;
    char *buf = result;
    if (buf == NULL)
        return -1; 
    iconv_t cd = iconv_open(out, in);
    char *inbuff = const_cast<char*>(src);
    size_t inbytesleft = size;
    char *outbuff = buf;
    size_t outbytesleft = max_len;
    size_t ret = iconv(cd, &inbuff, &inbytesleft, &result, &outbytesleft);
    if(ret == size_t(-1))
    {   
        ACE::write_n(ACE_STDOUT, "iconv failed:" , 15);
        ACE::write_n(ACE_STDOUT, strerror(errno) , strlen(strerror(errno)));
        ACE::write_n(ACE_STDOUT, src , size);
    }else
    {
        *r_size = ret;
        // ACE::write_n(ACE_STDOUT, "iconv success:" , 14);
        // ACE::write_n(ACE_STDOUT, result , ret);
    } 
    iconv_close(cd);
    //delete []buf;
    return ret;
}


char* getMimeType(char* fileName)
{
    char* extPos = strrchr(fileName,'.');
    char* extension;
    char* mimeType = "text/plain, charset=us-ascii";

    if(extPos == NULL){
        extension = "";
    }else{
        extension = extPos + 1;
    }

    /* Compare and return mimetype */
    switch(extension[0]){
        case 'b':
            if (strncmp(extension, "bmp", 3) == 0)
                mimeType = "image/bmp";
            if (strncmp(extension, "bin", 3) == 0)
                mimeType = "application/octet-stream";

            break;
        case 'c':
            if (strncmp(extension, "csh", 3) == 0)
                mimeType = "application/csh";
            if (strncmp(extension, "css", 3) == 0)
                mimeType = "text/css";

            break;
        case 'd':
            if (strncmp(extension, "doc", 3) == 0)
                mimeType = "application/msword";
            if (strncmp(extension, "dtd", 3) == 0)
                mimeType = "application/xml-dtd";
            break;
        case 'e':
            if (strncmp(extension, "exe", 3) == 0)
                mimeType = "application/octet-stream";
            break;
        case 'h':
            if (strncmp(extension, "html", 4) == 0 || strncmp(extension, "htm", 3) == 0)
                mimeType = "text/html";
            break;
        case 'i':
            if (strncmp(extension, "ico", 3) == 0)
                mimeType = "image/x-icon";
            break;
        case 'g':
            if (strncmp(extension, "gif", 3) == 0)
                mimeType = "image/gif";
            break;
        case 'j':
            if (strncmp(extension, "jpeg", 4) == 0 || strncmp(extension, "jpg", 3) == 0)
                mimeType = "image/jpeg";
            break;
        case 'l':
            if (strncmp(extension, "latex", 5) == 0)
                mimeType = "application/x-latex";
            break;
        case 'p':
            if (strncmp(extension, "png", 3) == 0)
                mimeType = "image/png";
            if (strncmp(extension, "pgm", 3) == 0)
                mimeType = "image/x-portable-graymap";
            break;  
        case 'r':
            if (strncmp(extension, "rtf", 3) == 0)
                mimeType  = "text/rtf";
            break;
        case 's':
            if (strncmp(extension, "svg", 3) == 0)
                mimeType = "image/svg+xml";
            if (strncmp(extension, "sh", 2) == 0)
                mimeType = "application/x-sh";
            break;
        case 't':
            if (strncmp(extension, "tar", 3) == 0)
                mimeType = "application/x-tar";
            if (strncmp(extension, "tex", 3) == 0)
                mimeType = "application/x-tex";
            if (strncmp(extension, "tif", 3) == 0 || strncmp(extension, "tiff", 4) == 0)
                mimeType = "image/tiff";
            if (strncmp(extension, "txt", 3) == 0)
                mimeType = "text/plain";
            break;
        case 'u':
            if (strncmp(extension, "utf8", 4) == 0)
                mimeType = "text/plain, charset=utf8";
            break;
        case 'x':
            if (strncmp(extension, "xml", 3) == 0)
                mimeType = "application/xml";
            break;
        default:
            break;
    }

    return mimeType;
}

int
ACE_TMAIN (int argc, ACE_TCHAR *argv[]) {
    ACE_INET_Addr server_addr;
    ACE_SOCK_Acceptor acceptor;
    ACE_SOCK_Stream peer;
    int buffer_size = 2048;
    
    int port = 80;
    if(argc>1) {
        ACE::write_n(ACE_STDOUT,argv[1],strlen(argv[1]));
        port = atoi(argv[1]);
    }
    if (server_addr.set(port) == -1) {
        ACE::write_n(ACE_STDOUT,"Fail to set port: 80",13);
        return 1;
    }
    if (acceptor.open(server_addr) == -1) {
        ACE::write_n(ACE_STDOUT,"Fail to open server_addr",21);
        return 1;
    }
    for (;;) {
        ACE::write_n(ACE_STDOUT,"waiting to connect",18);
        if (acceptor.accept(peer) == -1)
            return 1;
        peer.disable(ACE_NONBLOCK);
        ACE_Message_Block mb (buffer_size);
        char *head = mb.rd_ptr();
        for (;;) {
            ACE::write_n(ACE_STDOUT,"waiting to recv",15);
            // recv data
            int result = peer.recv (mb.wr_ptr (),
                                    buffer_size-(mb.wr_ptr ()-mb.rd_ptr ()));
            if (result > 0) {
                mb.wr_ptr (result);
                ACE::write_n(ACE_STDOUT, mb.wr_ptr() - result, result);
                if (ACE_OS::strchr (mb.rd_ptr (), '\n') > 0) {
                    if (ACE_OS::strncmp(mb.rd_ptr(), "GET", 3) == 0) {
                        mb.rd_ptr (4);
                        head = mb.rd_ptr ();
                        char* ptr = ACE_OS::strchr (head, ' ');
                        *ptr = '\0';
                        
                        char* filename = 0;
                        //remove /
                        filename = ACE_OS::strdup (head + 1);
                        auto_ptr<char> auto_filename (filename);
                        ACE::write_n(ACE_STDOUT, (const char *)filename, ptr - head - 1);
                        ACE::write_n(ACE_STDOUT, "\n", 1);
                       
                        // Is this a query?
                        if(strchr(filename,'?')!=NULL) {
                            ACE::write_n(ACE_STDOUT, "it is a query", 13);
                            ACE::write_n(ACE_STDOUT, (const char *)filename, strlen(filename));
                            // append?file=xxx/yyy/zz&key=value&key2=value2
                            if(strncmp(filename,"append",6) == 0)
                            {
                                ACE::write_n(ACE_STDOUT, "append request", 14);
                                char* keyvalue = strchr(filename,'?');
                                // skep ? mark
                                keyvalue = keyvalue + 1;
                                ACE_HANDLE handle = ACE_INVALID_HANDLE;
                                for(;;) {
                                    char* key = keyvalue;
                                    char* value = strchr(keyvalue,'=');
                                    *value = '\0';
                                    value = value + 1;
                                    if(strchr(value,'&') == NULL) {
                                        // Is this key value last one?
                                        keyvalue = NULL;

                                    } else {
                                        keyvalue = strchr(value,'&');
                                        *keyvalue = '\0';
                                        keyvalue = keyvalue + 1;
                                    }
                                    ACE_OS::write_n (ACE_STDOUT, "\n", 1);
                                    ACE_OS::write_n (ACE_STDOUT, key, strlen(key));
                                    ACE_OS::write_n (ACE_STDOUT, ",", 1);
                                    ACE_OS::write_n (ACE_STDOUT, value, strlen(value));
                                    ACE_OS::write_n (ACE_STDOUT, "\n", 1);
                                    
                                    if ( strncmp(key, "file", 4) == 0) {
                                        char* appendfile = value;
                                        ACE_stat stat;
                                        // Can we stat the file?
                                        if (ACE_OS::stat (appendfile, &stat) == -1) {
                                            ACE::write_n(ACE_STDOUT,"stat fail",9);
                                            break;
                                        }
                                        ssize_t size = stat.st_size;
                                        // Can we open the file?
                                        handle = ACE_OS::open (appendfile, O_CREAT | O_RDWR | O_APPEND);
                                        if (handle == ACE_INVALID_HANDLE) {
                                            ACE::write_n(ACE_STDOUT,"open fail",9);
                                            break;
                                        }
                                    } else {
                                        // Can we write the file?
                                        if (handle == ACE_INVALID_HANDLE) {
                                            ACE::write_n(ACE_STDOUT,"write fail",9);
                                            break;
                                        } 
                                        ACE_OS::write_n (handle, value, strlen(value));
                                        if(keyvalue != NULL) {
                                            ACE_OS::write_n (handle, "\t", 1);
                                        }
                                        // ACE_OS::write_n (ACE_STDOUT, value, strlen(value));
                                        // ACE_OS::write_n (ACE_STDOUT, "\t", 1);                                        
                                    }

                                    // Is there more keyvalue?
                                    if(keyvalue == NULL) {
                                        ACE_OS::write_n (handle, "\n", 1);
                                        break;
                                    }
                                } // end append
                                if (handle != ACE_INVALID_HANDLE) {
                                    ACE_OS::close(handle);
                                }
                                peer.send_n("HTTP/1.0 200 OK\n", 16);
                                ACE::write_n(ACE_STDOUT,"HTTP/1.0 200 OK\n", 16);
                        
                            }
                            break;
                        }
                        // It is not a query
                        if(strlen(filename)==0) {
                            ACE::write_n(ACE_STDOUT,"filename length error",21);
                            break;
                        }
                        
                        ACE_stat stat;
                        // Can we stat the file?
                        if (ACE_OS::stat (filename, &stat) == -1) {
                            ACE::write_n(ACE_STDOUT,"stat fail",9);
                            break;
                        }
                        ssize_t size = stat.st_size;
                        if (size == 0) {
                            ACE::write_n(ACE_STDOUT,"file size error",15);
                            break;
                        }
                        // Can we open the file?
                        ACE_HANDLE handle = ACE_OS::open (filename, O_RDONLY);
                        if (handle == ACE_INVALID_HANDLE) {
                            ACE::write_n(ACE_STDOUT,"open fail",9);
                            break;
                        }
                        char* file = new char[size];
                        auto_ptr<char> auto_file (file);
                        ACE_OS::read_n (handle, file, size);
                        ACE::write_n(ACE_STDOUT,(const char *)file,size);
                        peer.send_n("HTTP/1.0 200 OK\n", 16);
                        ACE::write_n(ACE_STDOUT,"HTTP/1.0 200 OK\n", 16);
                        char* m_mimeType = getMimeType(filename);
                        peer.send_n("Content-type: ", 14);
                        peer.send_n(m_mimeType, strlen(m_mimeType));
                        peer.send_n("\n", 1);
                        // if (strncmp(m_mimeType,"data/unknown", 12) == 0) {
                        //     ssize_t r_size = size * 2 + 1;
                        //     char* result = new char[r_size];
                        //     memset(result,0,r_size);
                        //     auto_ptr<char> auto_result (result);
                        //     ssize_t ret = s_iconv(file, size,result, &r_size,"gbk","utf8//IGNORE");
                        //     if (r_size > 0) {
                        //         file = result;
                        //         size = r_size;    
                        //     }
                        // }
                        ACE::write_n(ACE_STDOUT, "Content-type: ", 14);
                        ACE::write_n(ACE_STDOUT, m_mimeType, strlen(m_mimeType));
                        ACE::write_n(ACE_STDOUT, "\n", 1);
                        char* content_len = new char[16];
                        auto_ptr<char> auto_content_len (content_len);
                        ACE_OS::itoa (size, content_len, 10);
                        peer.send_n("Content-length: ", 16);
                        peer.send_n(content_len, strlen(content_len));
                        peer.send_n("\n\n", 2);
                        peer.send_n(file, size);
                        ACE::write_n(ACE_STDOUT, "Content-length: ", 16);
                        ACE::write_n(ACE_STDOUT, content_len, strlen(content_len));
                        ACE::write_n(ACE_STDOUT, "\n\n", 2);
                        ACE::write_n(ACE_STDOUT, file, size);
                    }
                    break;
                }//end if get request
            }//end if get data
            else
            {
                break;
            }
        }//end for read data
        peer.close();
    }//end for listen
    return acceptor.close()==-1?1:0;
}
