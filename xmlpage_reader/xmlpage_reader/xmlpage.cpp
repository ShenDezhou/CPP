#include "xmlpage.hpp"
void free_xml(XmlPage *&page)
{
	if(page->url != NULL)
	{
		free(page->url);
	}
	if(page->objid != NULL)
	{
		free(page->objid);
	}
	if(page->doc != NULL)
	{
		free(page->doc);
	}
	if(page != NULL)
	{
		free(page);
	}
}
