/*
 * btleaf_page.cc - implementation of class BTLeafPage
 *
 */

#include "btleaf_page.h"

const char* BTLeafErrorMsgs[] = {
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
  return OK;
}

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
  return OK;
}

Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 
  return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
  return OK;
}
