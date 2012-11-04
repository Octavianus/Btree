/*
 * btleaf_page.cc - implementation of class BTLeafPage
 *
 */

#include "btleaf_page.h"

const char* BTLeafErrorMsgs[] = { 
	"Insert Data Entry Failed (BTLeafPage::insertRec)",
	"Get First Failed (BTLeafPage::get_first)",
	"Get Next Failed (BTLeafPage::get_next)",
	"Get Data Rid Failed (BTLeafPage::get_data_rid)",
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
	KeyDataEntry *target = new KeyDataEntry;
	int reclen;
	Status st;
	Datatype *dt = new Datatype;
	dt->rid = dataRid;
	
	// package the data entry
	make_entry(target, key_type, key, LEAF, *dt, &reclen);

	if ( OK != (st=(((SortedPage *)this)->insertRecord(key_type, (char *)target, reclen, rid))) )
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE,INSERT_REC_FAILED); 		

  	return OK;
}

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
    Datatype *dt = new Datatype;
    dt->rid = dataRid;
	int keylen = get_key_length(key,key_type); 
	int reclen = get_key_data_length(key,key_type,LEAF);

	// Perform a linear scan for key
	for (int i = slotCnt-1; i > 0; i--)
	{
		char *key_i = data + slot[i].offset;
		
		if (keyCompare((void*)key_i, key, key_type) == 0)
		{
			// Get the key,data pair
			get_key_data(key,dt,((KeyDataEntry *)key_i),reclen,LEAF);			
			//dataRid = ((RID *)(((char *)key_i)+keylen));
			dataRid = dt->rid;
			return OK;
		}
	}
	return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_DATA_RID_FAILED);
}

Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 
	char *recptr;
	int reclen;
	Datatype *dt = new Datatype;
	dt->rid = dataRid;
	KeyDataEntry *kd = new KeyDataEntry;

	// Get the rid of the first record
	if ( OK != (((HFPage*)this)->firstRecord(rid)) ) 
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

	// Pointer of the record from page
	if ( OK != (((HFPage*)this)->returnRecord(rid, recptr, reclen)) )
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);
	
	kd = (KeyDataEntry *)recptr;

	// Get the key,data pair
	get_key_data(key,dt,kd,reclen,LEAF);
	
	dataRid = dt->rid;
	
  	return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
	RID nextRid;
    Status st;
    char *recptr;
    int reclen;
	Datatype *dt = new Datatype;
	dt->rid = dataRid;

    // Get the rid of the next record
    if ( OK != (st=((HFPage*)this)->nextRecord(rid, nextRid)) )
		return DONE;

    // Pointer of the record from page
    if ( OK != (((HFPage*)this)->returnRecord(nextRid, recptr, reclen)) )
        return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_NEXT_FAILED);

    // Get the key,data pair
    get_key_data(key,dt,((KeyDataEntry *)recptr),reclen,LEAF);

	dataRid = dt->rid;

	// also set the currid to nextrid
	rid = nextRid; 	

	return OK;
}

Status BTLeafPage::deleteKey(const RID& rid)
{
	Status st;

    if ( OK != (st=(((SortedPage *)this)->deleteRecord(rid))) )
         return MINIBASE_CHAIN_ERROR(SORTEDPAGE, st);
 
	return OK;


#if 0
    int keylen  = get_key_length(key,key_type);

    // Perform a linear scan for key
    for (int i = slotCnt-1; i > 0; i--)
    {
        char *key_i = data + slot[i].offset;

        if (keyCompare((void*)key_i, key, key_type) == 0)
        {
			if ( OK != (st=(((SortedPage *)this)->deleteRecord(datarid))) )
				return MINIBASE_CHAIN_ERROR(SORTEDPAGE, st);
			else
            	return OK;
        }
    }
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, DELETE_KEY_FAILED);
#endif
}
