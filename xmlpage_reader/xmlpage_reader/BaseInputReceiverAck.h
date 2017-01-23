/** 
 * @file BaseInputReceiver.h
 * @brief the interface for servers that receive inputs
 * @author fishy@sohu-rd.com
 * @date 2008-05-06
 */

#pragma once

#ifndef __BASE_INPUT_RECEIVER_ACK_H
#define __BASE_INPUT_RECEIVER_ACK_H

#include <sys/uio.h>
#include <ace/Time_Value.h>

#include "NetworkInputAck.h"

/** 
 * @brief the base class for servers that receive data from input
 */
class BaseInputReceiverAck {
public:
	virtual ~BaseInputReceiverAck() {}

public:
	/** 
	 * @brief put data into the receiver processing queue
	 * 
	 * @param data raw data to be put
	 * @param copy true for copy data while putting (caller can free it), or false for BaseInputReceiver to free it after use
	 * @param tv the timeout time, NULL for blocking wait,
	 * 
	 * @return 0 on successful, or -1 if failed (queue full)
	 * @note caller may free the data after this function returns, so sub-class should keep a copy of the content
	 */
	virtual int putData(const iovec &data, bool copy = true, ACE_Time_Value *tv = NULL) = 0;
	/** 
	 * @brief the function to handle when a new connection is going to be made, sub-class can limit connection num here
	 * 
	 * @param con the connection to be made
	 * 
	 * @return 0 to accept this connection or -1 to refuse it
	 * @sa delConnection()
	 */
	virtual int addConnection(NetworkInputAckHandler* con) { return 0; }
	/** 
	 * @brief the function to handle when a connection is closing
	 * 
	 * @param con the connection to be closed
	 * 
	 * @return -1 if connection not found or other errors, 0 for ok to close
	 * @sa addConnection()
	 */
	virtual int delConnection(NetworkInputAckHandler* con) { return 0; }
};

#endif	/* __BASE_INPUT_RECEIVER_H */

