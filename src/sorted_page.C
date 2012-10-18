/*
 * sorted_page.cc - implementation of class SortedPage
 *
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"
#ifdef MULTIUSER
#include "recovery_mgr.h"
#endif

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  "OK",
  "Insert Record Failed (SortedPage::insertRecord)",
  "Delete Record Failed (SortedPage::deleteRecord",
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key is a void * pointer to a key.
 * 
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid)
{
  Status status;
  int i;
 
  // ASSERTIONS:
  // - the slot directory is compressed -> inserts will occur at the end
  // - slotCnt gives the number of slots used

  // general plan:
  //    1. Insert the record into the page,
  //       which is then not necessarily any more sorted
  //    2. Sort the page by rearranging the slots (insertion sort)

  status = HFPage::insertRecord(recPtr, recLen, rid);
  if (status != OK)
    return MINIBASE_FIRST_ERROR(SORTEDPAGE, INSERT_REC_FAILED);
  
  assert(rid.slotNo == (slotCnt-1)); 
  
#ifdef MULTIUSER
 char tmp_buf[DPFIXED];
 memcpy(tmp_buf,(void*)&slot[0], DPFIXED);
#endif

  // performs a simple insertion sort
  for (i=slotCnt-1; i > 0; i--) 
  {
    char *key_i = data + slot[i].offset;
    char *key_iplus1 = data + slot[i-1].offset;
    
    if (keyCompare((void*)key_i, (void*) key_iplus1, key_type) < 0)
    {
      // switch slots:
      slot_t tmp_slot;
      tmp_slot  = slot[i];
      slot[i]   = slot[i-1];
      slot[i-1] = tmp_slot;

    } else {

      // end insertion sort
      break;
    }
  }

  // ASSERTIONS:
  // - record keys increase with increasing slot number (starting at slot 0)
  // - slot directory compacted

  rid.slotNo = i;

#ifdef MULTIUSER
   status = MINIBASE_RECMGR->WriteUpdateLog(DPFIXED, curPage,sizeof data,
                         tmp_buf,(char*)&slot[0], (Page*) this);
   if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE,status);
#endif

  return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
  Status status;
  
  status=HFPage::deleteRecord(rid);
  
  if (status == OK)
    HFPage::compact_slot_dir();
  else
    return MINIBASE_FIRST_ERROR(SORTEDPAGE, DELETE_REC_FAILED);

  // ASSERTIONS:
  // - slot directory is compacted

  return OK;
}

int SortedPage::numberOfRecords()
{
  return slotCnt;
}
