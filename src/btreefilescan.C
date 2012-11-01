/*
 * btreefilescan.cc - function members of class BTreeFileScan
 *
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"
#include "index.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::~BTreeFileScan ()
{
	
}

int BTreeFileScan::keysize() 
{
	return key_size;
   
}

Status BTreeFileScan::get_next (RID & rid, void* keyptr)
{
	Status st;
	void *key;
	int reclen;
	RID datarid;
	Page *page;
	char *recptr;

	if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
		return MINIBASE_CHAIN_ERROR(BUFMGR,st);

	// Case 1: scan all the data entries 
	if ( (lo_key == NULL) && (hi_key == NULL) )
	{
		// copies out record with currentRID into keyptr
    	if ( OK != (st=((HFPage *)page)->getRecord(currentRid, (char *)keyptr, reclen)) )
 			return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

	    // Get the rid of the next record
	    if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, currentRid)) )
    	{
			// Unpin current page
		    if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
		        return MINIBASE_CHAIN_ERROR(BUFMGR,st);

			// we exhausted the entries on this page. Get the next page.
			if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
				return DONE;
			
			// Pin the next page	
			if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) ) 
				return MINIBASE_CHAIN_ERROR(BUFMGR, st);
		}		
		
	    return OK;		
		
	}
	// Case 2: range scan from min to hi_key
	if ( (lo_key == NULL) && (hi_key != NULL) )
	{
		// copies out record with currentRID into keyptr
    	if ( OK != (st=((HFPage *)page)->getRecord(currentRid, (char *)keyptr, reclen)) )
 			return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

	    // Get the rid of the next record
	    if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, currentRid)) )
    	{
            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

			// we exhausted the entries on this page. Get the next page.
			if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
				return DONE;
			
			// Pin the next page	
			if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) ) 
				return MINIBASE_CHAIN_ERROR(BUFMGR, st);
			
		}		
		
		Datatype *dt = ((Datatype *&)datarid);
	    // Get the key,data pair
	    get_key_data(key,dt,((KeyDataEntry *)recptr),reclen,LEAF);
	    
		if ( key > hi_key)			
			return DONE;

		return OK;
	}

	// Case 3: range scane from lo_key to max
    if ( (lo_key != NULL) && (hi_key == NULL) )
    {   
		// copies out record with currentRID into keyptr
    	if ( OK != (st=((HFPage *)page)->getRecord(currentRid, (char *)keyptr, reclen)) )
 			return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

	    // Get the rid of the next record
	    if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, currentRid)) )
    	{
            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

			// we exhausted the entries on this page. Get the next page.
			if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
				return DONE;
			
			// Pin the next page	
			if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) ) 
				return MINIBASE_CHAIN_ERROR(BUFMGR, st);
			
		}		
		
		return OK;
    }

    // Case 4: exact match ( might not unique)
    if ( (lo_key != NULL) && (hi_key != NULL) && (lo_key == hi_key) )
    {
        // copies out record with currentRID into keyptr
        if ( OK != (st=((HFPage *)page)->getRecord(currentRid, (char *)keyptr, reclen)) )
            return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the rid of the next record
        if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, currentRid)) )
        {
            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // we exhausted the entries on this page. Get the next page.
            if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
                return DONE;

            // Pin the next page
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR, st);

        }
        
		if ( key == lo_key)
            return OK;
		else
			return DONE;
    }

    // Case 5: range scan from lo_key to hi_key
    if ( (lo_key != NULL) && (hi_key != NULL) && (lo_key < hi_key) )
    {
        // copies out record with currentRID into keyptr
        if ( OK != (st=((HFPage *)page)->getRecord(currentRid, (char *)keyptr, reclen)) )
            return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the rid of the next record
        if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, currentRid)) )
        {
            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // we exhausted the entries on this page. Get the next page.
            if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
                return DONE;

            // Pin the next page
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR, st);

        }

		Datatype *dt = ((Datatype *&)datarid);
		// Get the key,data pair
        get_key_data(key,dt,((KeyDataEntry *)recptr),reclen,LEAF);

        if ( (key >= lo_key) && (key <= hi_key) )
			return OK;
		else
            return DONE;
    }
 
}

Status BTreeFileScan::delete_current ()
{
	Page *page;
	RID temp = currentRid;
	Status st;

	if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
        return MINIBASE_CHAIN_ERROR(BUFMGR,st);

    // point currentRid to the next record.
    if ( OK != (st=((HFPage *)page)->nextRecord(temp, currentRid)) )
		return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);		

	if ( OK != (st=((SortedPage *)page)->deleteRecord(temp)) ) 
		return MINIBASE_CHAIN_ERROR(SORTEDPAGE, st);

    if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
        return MINIBASE_CHAIN_ERROR(BUFMGR,st);
	
}	
