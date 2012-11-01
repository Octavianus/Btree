/*
 * btindex_page.cc - implementation of class BTIndexPage
 *
 */

#include "btindex_page.h"
#include "heapfile.h"
#include "buf.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
    "Insert Data Entry Failed (BTIndexPage::insertRec)",
    "Get First Failed (BTIndexPage::get_first)",
    "Get Next Failed (BTIndexPage::get_next)",
    "Get Page No Failed (BTIndexPage::get_page_no)",
};
static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
    KeyDataEntry *target = new KeyDataEntry;
    int reclen;
    Status st;
    Datatype *dt = new Datatype;
    dt->pageNo = pageNo;
	
    // package the data entry
    make_entry(target, key_type, key, INDEX, *dt, &reclen);

    if ( OK != (st=(((SortedPage *)this)->insertRecord(key_type, (char *)target, reclen, rid))) )
        return MINIBASE_FIRST_ERROR(BTINDEXPAGE,INSERT_REC_FAILED);

    return OK;
}

Status BTIndexPage::deleteKey (RID& rid)
{
    Status st;

    if ( OK != (st=(((SortedPage *)this)->deleteRecord(rid))) )
         return MINIBASE_CHAIN_ERROR(SORTEDPAGE, st);

	return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
	Page *page;
	RID rid;
	void *tmp_key;
	PageId tmp_pageno;
	Status st;

	if ( key == NULL )
	{
		pageNo = ((HFPage*)this)->page_no();
		return OK;
	}

	// If key is less than the first key, return pageNo of the first record
	if ( OK != this->get_first(rid,tmp_key,tmp_pageno))
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);

	if ( keyCompare(key, tmp_key, key_type) < 0 )
	{
		// key < first key; return the pageno of the leftmost link from first rec
		// recursively search the next
		pageNo = this->getLeftLink();
		return OK;
		#if 0
		if ( OK != (st=MINIBASE_BM->newPage(pageNo, (Page*&)page)) )
			return MINIBASE_CHAIN_ERROR(BUFMGR, st);
		if ( OK != ((BTIndexPage *)page)->get_page_no(key,key_type,pageNo))
			return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);
		if ( OK != (st=MINIBASE_BM->unpinPage(pageNo)) )
			return MINIBASE_CHAIN_ERROR(BUFMGR, st);		
		#endif
	}
	else
	{
		// keep looking for a key' such that
		// key' <= key < key''	
		
		// compare the key with the last record
		do
		{
			if ( (keyCompare(tmp_key, key, key_type) < 0) || (keyCompare(tmp_key, key, key_type) == 0) )
			{
				void *sec_tmp_key;
				PageId sec_tmp_pageno;
				if ( OK == this->get_next(rid,sec_tmp_key,sec_tmp_pageno) )
				{
					if ( keyCompare(key, sec_tmp_key, key_type) < 0 )
					{
						pageNo = tmp_pageno;
						return OK;
						#if 0
						if ( OK != (st=MINIBASE_BM->newPage(pageNo, (Page*&)page)) )
							return MINIBASE_CHAIN_ERROR(BUFMGR, st);
						if ( OK != ((BTIndexPage *)page)->get_page_no(key,key_type,pageNo))
							return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);
						if ( OK != (st=MINIBASE_BM->unpinPage(pageNo)) )
							return MINIBASE_CHAIN_ERROR(BUFMGR, st);						
						#endif
					}
				}				
			}
		}while ( OK != this->get_next(rid,tmp_key,tmp_pageno) );
	
		// we ran out of records on this page. Use the last pageno
		pageNo = tmp_pageno;
		return OK;
		#if 0
		if ( OK != (st=MINIBASE_BM->newPage(pageNo, (Page*&)page)) )
			return MINIBASE_CHAIN_ERROR(BUFMGR, st);
		if ( OK != ((BTIndexPage *)page)->get_page_no(key,key_type,pageNo))
			return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);
		if ( OK != (st=MINIBASE_BM->unpinPage(pageNo)) )
			return MINIBASE_CHAIN_ERROR(BUFMGR, st);				
		#endif
	}
   
	return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);					
}
    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
    char *recptr;
    int reclen;
    Datatype *dt = new Datatype;
    dt->pageNo = pageNo;
    KeyDataEntry *kd = new KeyDataEntry;

    // Get the rid of the first record
    if ( OK != (((HFPage*)this)->firstRecord(rid)) )
        return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);
    // Pointer of the record from page
    if ( OK != (((HFPage*)this)->returnRecord(rid, recptr, reclen)) )
        return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

    kd = (KeyDataEntry *)recptr;

    // Get the key,data pair
    get_key_data(key,dt,kd,reclen,INDEX);

    pageNo = dt->pageNo;

    return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
    RID nextRid;
    Status st;
    char *recptr;
    int reclen;
    Datatype *dt = new Datatype;
    dt->pageNo = pageNo;

    // Get the rid of the next record
    if ( OK != (((HFPage*)this)->nextRecord(rid, nextRid)) )
        if ( st == DONE)
            return MINIBASE_FIRST_ERROR(BTLEAFPAGE, INDEXNOMORERECS);

    // Pointer of the record from page
    if ( OK != (((HFPage*)this)->returnRecord(nextRid, recptr, reclen)) )
        return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_NEXT_FAILED);

    // Get the key,data pair
    get_key_data(key,dt,((KeyDataEntry *)recptr),reclen,INDEX);

	pageNo = dt->pageNo;

	// also set the currid to nextrid
	rid = nextRid;

	return OK;
}
