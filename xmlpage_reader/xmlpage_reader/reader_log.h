#ifndef	READER_LOG_H_
#define READER_LOG_H_

#include <unistd.h>
#include <stdarg.h>

#ifdef DEBUG
#define CODE_INFO               __FILE__, __PRETTY_FUNCTION__, __LINE__
#define CODE_INFO_FORMAT        "[%s:%s:%u] "
#else
#define CODE_INFO               ""
#define CODE_INFO_FORMAT        "%s"
#endif


#define reader_log_error(fmt, ...)\
({\
    time_t __time_buf__;\
    tm __localtime_buf__;\
    char __strftime_buf__[sizeof("19820429 23:59:59")];\
    time(&__time_buf__);\
    localtime_r(&__time_buf__, &__localtime_buf__);\
    strftime(__strftime_buf__, sizeof("19820429 23:59:59"), "%Y%m%d %H:%M:%S", &__localtime_buf__);\
    fprintf(stderr, "[%s][%08lX]" CODE_INFO_FORMAT fmt,\
            __strftime_buf__, pthread_self(), CODE_INFO, ##__VA_ARGS__);\
})

#define reader_log_info(fmt, ...)\
({\
    time_t __time_buf__;\
    tm __localtime_buf__;\
    char __strftime_buf__[sizeof("19820429 23:59:59")];\
    time(&__time_buf__);\
    localtime_r(&__time_buf__, &__localtime_buf__);\
    strftime(__strftime_buf__, sizeof("19820429 23:59:59"), "%Y%m%d %H:%M:%S", &__localtime_buf__);\
    fprintf(stdout, "[%s] [%08lX]" CODE_INFO_FORMAT fmt,\
            __strftime_buf__, pthread_self(), CODE_INFO, ##__VA_ARGS__);\
})



#endif
