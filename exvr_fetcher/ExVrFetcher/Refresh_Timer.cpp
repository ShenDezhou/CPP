#include "Refresh_Timer.h"
#include "Configuration.hpp"
#include "Platform/log.h"


void Refresh_Timer::Update(int minute)
{
    m_Mask = 0;
    time_t now = time(NULL);
    m_RestTime = 0;

    for (int i = 0; i < 0 + 1; i++) 
    {
        //当前时间，上一次更新时间，
        time_t t = now - m_PrevRefreshTicks[i] - minute * 60;
        //time_t t = now - m_PrevRefreshTicks[i] - 1 * 60;
        if (t >= 0)//已经过了重抓时间 
        {
            m_Mask |= (1 << i);
            m_PrevRefreshTicks[i] = now;
        } 
        else 
        {
            if(m_RestTime == 0)
                m_RestTime = -t;
            m_RestTime = m_RestTime < -t ? m_RestTime : -t;
        }
    }

    if(m_Mask)//必须重抓，则为0
        m_RestTime = 0;
}
