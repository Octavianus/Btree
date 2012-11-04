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
	Keytype key;
	int reclen;
	Page *page;
	char *recptr;
	Datatype *dt = new Datatype;

	currentRid = nextrid;

	if ( this->valid == 0 )
		return DONE;

	if ( currentpage == INVALID_PAGE )
		return DONE;

	if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
		return MINIBASE_CHAIN_ERROR(BUFMGR,st);

	// Case 1: scan all the data entries 
	if ( (lo_key == NULL) && (hi_key == NULL) )
	{
		// copies out record with currentRID into keyptr
    	if ( OK != (st=((HFPage *)page)->returnRecord(currentRid, recptr, reclen)) )
 			return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the key,data pair
        get_key_data(keyptr,dt,((KeyDataEntry *)recptr),reclen,LEAF);

        rid = dt->rid;

	    // Get the rid of the next record
	    if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, nextrid)) )
    	{
			PageId temp = currentpage;

			// we exhausted the entries on this page. Get the next page.
			if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
			{
	            // Unpin current page
    	        if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
        	        return MINIBASE_CHAIN_ERROR(BUFMGR,st);
				return OK;
			}

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Pin this page and Update the currentRid to first record.
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            if ( OK != ((HFPage *)page)->firstRecord(nextrid) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

		}		
	    else
		{	
        	// Unpin current page
	        if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
    	        return MINIBASE_CHAIN_ERROR(BUFMGR,st);
		}
	    return OK;		
		
	}
	// Case 2: range scan from min to hi_key
	if ( (lo_key == NULL) && (hi_key != NULL) )
	{

		currentRid = nextrid;
		
        // copies out record with currentRID into keyptr
        if ( OK != (st=((HFPage *)page)->returnRecord(currentRid, recptr, reclen)) )
            return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the key,data pair
        get_key_data(keyptr,dt,((KeyDataEntry *)recptr),reclen,LEAF);

        rid = dt->rid;

		// Stop if currentRid's key > hi_key
        if ( keyCompare(keyptr,hi_key,this->ktype) > 0 )
            return DONE;

	    // Get the rid of the next record
	    if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, nextrid)) )
    	{
            PageId temp = currentpage;

            // we exhausted the entries on this page. Get the next page.
            if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
                return DONE;

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Pin this page and Update the currentRid to first record.
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            if ( OK != ((HFPage *)page)->firstRecord(nextrid) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);
		}
		else
		{
        	if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
            	return MINIBASE_CHAIN_ERROR(BUFMGR,st);
		}
		return OK;
	}

	// Case 3: range scan from lo_key to max
    if ( (lo_key != NULL) && (hi_key == NULL) )
    {   

		currentRid = nextrid;

	    if ( currentpage == INVALID_PAGE )
    	    return DONE;

		// copies out record with currentRID into keyptr
    	if ( OK != (st=((HFPage *)page)->returnRecord(currentRid, recptr, reclen)) )
 			return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the key,data pair
        get_key_data(keyptr,dt,((KeyDataEntry *)recptr),reclen,LEAF);

        rid = dt->rid;

	    // Get the rid of the next record
	    if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, nextrid)) )
    	{
            PageId temp = currentpage;

            // we exhausted the entries on this page. Get the next page.
            if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
            {
                // Unpin current page
                if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
                    return MINIBASE_CHAIN_ERROR(BUFMGR,st);
                return OK;
            }

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Pin this page and Update the currentRid to first record.
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            if ( OK != ((HFPage *)page)->firstRecord(nextrid) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

        }
		else
		{
        	if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
            	return MINIBASE_CHAIN_ERROR(BUFMGR,st);
		}
		return OK;
    }

    // Case 4: exact match ( might not unique)
    if ( (lo_key != NULL) && (hi_key != NULL) && (keyCompare(lo_key,hi_key,this->ktype) == 0) )
    {
		currentRid = nextrid;
        // copies out record with currentRID into keyptr
        if ( OK != (st=((HFPage *)page)->returnRecord(currentRid, recptr, reclen)) )
            return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the key,data pair
        get_key_data(keyptr,dt,((KeyDataEntry *)recptr),reclen,LEAF);

        rid = dt->rid;

        // Stop if currentRid's key != lo_key or hi_key
        if ( (keyCompare(keyptr,hi_key,this->ktype) < 0) || (keyCompare(keyptr,hi_key,this->ktype) > 0) )
            return DONE;

        // Get the rid of the next record
        if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, nextrid)) )
        {
            PageId temp = currentpage;

            // we exhausted the entries on this page. Get the next page.
            if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
                return DONE;

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Pin this page and Update the currentRid to first record.
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            if ( OK != ((HFPage *)page)->firstRecord(nextrid) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

        }
        else
        {
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);
        }
		return OK;

    }

    // Case 5: range scan from lo_key to hi_key
    if ( (lo_key != NULL) && (hi_key != NULL) && (keyCompare(lo_key,hi_key,this->ktype) < 0) )
    {
		currentRid = nextrid;
        // copies out record with currentRID into keyptr
        if ( OK != (st=((HFPage *)page)->returnRecord(currentRid, recptr, reclen)) )
            return MINIBASE_CHAIN_ERROR(HEAPPAGE, st);

        // Get the key,data pair
        get_key_data(keyptr,dt,((KeyDataEntry *)recptr),reclen,LEAF);

        rid = dt->rid;

        // Stop if currentRid's key > hi_key
        if ( keyCompare(keyptr,hi_key,this->ktype) > 0 ) 
            return DONE;

        // Get the rid of the next record
        if ( OK != (st=((HFPage *)page)->nextRecord(currentRid, nextrid)) )
        {
            PageId temp = currentpage;

            // we exhausted the entries on this page. Get the next page.
            if ( INVALID_PAGE == (currentpage = ((HFPage *)page)->getNextPage()) )
                return DONE;

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Pin this page and Update the currentRid to first record.
            if ( OK != (st=MINIBASE_BM->pinPage(currentpage, (Page*&) page)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            if ( OK != ((HFPage *)page)->firstRecord(nextrid) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

            // Unpin current page
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);

        }
        else
        {
            if ( OK != (st=MINIBASE_BM->unpinPage(currentpage, TRUE)) )
                return MINIBASE_CHAIN_ERROR(BUFMGR,st);
        }
        return OK;
    }

}

Status BTreeFileScan::delete_current ()
{
	Page *page;
	PageId temp;
	
	// If we are on the same page. Do this.
	if ( currentRid.pageNo == nextrid.pageNo )
	{
		nextrid = currentRid;
		temp = currentpage;
	}
	else
	{
		temp = currentRid.pageNo;
	}
	
	Status st;

	if ( OK != (st=MINIBASE_BM->pinPage(temp, (Page*&) page)) )
        return MINIBASE_CHAIN_ERROR(BUFMGR,st);
	
	if ( OK != (st=((SortedPage *)page)->deleteRecord(currentRid)) ) 
		return MINIBASE_CHAIN_ERROR(SORTEDPAGE, st);

    if ( OK != (st=MINIBASE_BM->unpinPage(temp, TRUE)) )
        return MINIBASE_CHAIN_ERROR(BUFMGR,st);
	
}	
