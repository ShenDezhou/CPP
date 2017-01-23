#pragma once

#include "Platform/docId/docId.h"

class Utils {
public:
	static bool objid2docid(const char *objid, gDocID_t &docID);
};
