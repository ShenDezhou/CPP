#include "send_file_check.h"
#include "Platform/bchar_utils.h"
#include "base.h"

int SendFileCheck::init(config *m_cfg)
{
	m_cfg->read_value("CHECK_OVER_TIME", frequency);
	m_cfg->read_value("CHECK_TIME_INTERVAL", time_interval);
    return open();
}

int SendFileCheck::open(void *)
{
    return activate(THR_NEW_LWP, 1);
}

int SendFileCheck::svc()
{
	while (stop_signal == 0)
    {
        sleep(time_interval);
        g_xmlItemMgr.check_time(frequency);
    }
	return 0;
}

void SendFileCheck::stop_task()
{
	flush();
	wait();
	return;
}
