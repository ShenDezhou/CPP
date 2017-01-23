#ifndef _REFRESH_TIMER_H_
#define _REFRESH_TIMER_H_

//#include "Base_Class.h"
#include <ace/Singleton.h>
#include <ace/Synch.h>

class Refresh_Timer {

public:
	Refresh_Timer() : m_Mask(0), m_RestTime(0)
	{ memset(m_PrevRefreshTicks, 0, sizeof(m_PrevRefreshTicks)); }

	bool shouldUpdate(const int prd = 0) 
	{ return ((1 << prd) & m_Mask) != 0; }

	time_t getRestTime()
	{ return m_RestTime; }

    void Update(int minute);
    
private:
	time_t m_PrevRefreshTicks[1];
	long m_Mask;
	time_t m_RestTime;

private:
};

#endif
