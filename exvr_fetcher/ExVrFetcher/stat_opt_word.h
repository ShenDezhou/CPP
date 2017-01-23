#ifndef _STAT_OPT_WORD_H
#define _STAT_OPT_WORD_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <string>
#include <map>
#include <set>
#include "Platform/log.h"
#include "Platform/signal/sig.h"
#include "Platform/bchar_cxx.h"
#include <Platform/encoding.h>

#include "database.h"
#include "ExVrFetcher.h"

int stat_opt_word(int vr_id, int res_id, CMysql *mysql_vr);


#endif
