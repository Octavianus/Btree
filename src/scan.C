/*
 * scan.C - implementation of class Scan for HeapFile project.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

// *******************************************
Scan::Scan (HeapFile* hf, Status& status)
{
    status = init(hf);
}

// *******************************************
Scan::~Scan()
{
    reset();
}

// *******************************************
Status Scan::init (HeapFile *hf)
{
    _hf = hf;

    return firstDataPage();
}

// *******************************************
Status Scan::reset()
{
    Status st = OK;

    if (datapage != NULL) {
        st = MINIBASE_BM->unpinPage(datapageId);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
    }
    datapageId = 0;
    datapage = NULL;

    if (dirpage != NULL) {
        st = MINIBASE_BM->unpinPage(dirpageId);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
    }
    dirpage = NULL;

    nxtUserStatus = OK;

    return st;
}

// *******************************************
Status Scan::firstDataPage()
{
    DataPageInfo dpinfo;
    int          dpinfoLen;
    Status       st;

    // copy data about first directory page
    dirpageId = _hf->_firstDirPageId;  
    nxtUserStatus = OK;

#ifdef MULTIUSER
    _scan_pgid = dirpageId;
    st = MINIBASE_LOCKMGR->lock_page(_scan_pgid,Shared);
    if (st != OK)
        return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
#endif

    // get first directory page and pin it
    st = MINIBASE_BM->pinPage(dirpageId, (Page *&) dirpage);
    if (st != OK)
        return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
    
    // now try to get a pointer to the first datapage
    st = dirpage->firstRecord(datapageRid);

    if (st == OK) {
        // there is a datapage record on the first directory page:
        st = dirpage->getRecord(datapageRid, (char *)&dpinfo, dpinfoLen);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
    
        if (dpinfoLen != sizeof(DataPageInfo)) {
            st = FAIL;
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
        }
    
        datapageId = dpinfo.pageId;

    } else {

    // the first directory page is the only one which can possibly remain
    // empty: therefore try to get the next directory page and
    // check it. The next one has to contain a datapage record, unless
    // the heapfile is empty:

        PageId nextDirPageId;
    
        nextDirPageId = dirpage->getNextPage();

        if (nextDirPageId != INVALID_PAGE) {
            st = MINIBASE_BM->unpinPage(dirpageId, FALSE);
            dirpage = 0;
            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st);

#ifdef MULTIUSER
            _scan_pgid = nextDirPageId;
            st = MINIBASE_LOCKMGR->lock_page(_scan_pgid,Shared);
            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
#endif

            st = MINIBASE_BM->pinPage(nextDirPageId, (Page*& )dirpage);
            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
      
            // now try again to read a data record:
            st = dirpage->firstRecord(datapageRid);

            if (st == OK) {
                st = dirpage->getRecord(datapageRid,
                                (char *) &dpinfo, dpinfoLen);
                if (st != OK)
                    return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
                
                if (dpinfoLen != sizeof(DataPageInfo))
                    return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
                
                datapageId = dpinfo.pageId;
      
            } else {
                // heapfile empty
                datapageId = INVALID_PAGE;
            }

        } else {
            // heapfile empty
            datapageId = INVALID_PAGE;
        }    
    }

    datapage = NULL;

      // Actually get the page
    st = nextDataPage();
    if ((st != OK) && (st != DONE))
        return MINIBASE_CHAIN_ERROR( HEAPFILE, st);

    return OK;

  // ASSERTIONS:
  // - first directory page pinned
  // - this->dirpageId has Id of first directory page
  // - this->dirpage valid
  // - if heapfile empty:
  //    - this->datapage == NULL, this->datapageId==INVALID_PAGE
  // - if heapfile nonempty:
  //    - this->datapage == NULL, this->datapageId, this->datapageRid valid
  //    - first datapage is not yet pinned
}

// *******************************************
// Retrieve the next data page
Status Scan::nextDataPage()
{
    DataPageInfo dpinfo;
    int dpinfoLen;

    Status st;
    Status nextDataPageStatus;
    PageId nextDirPageId;
  

  // ASSERTIONS:
  // - this->dirpageId has Id of current directory page
  // - this->dirpage is valid and pinned
  // (1) if heapfile empty:
  //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
  // (2) if overall first record in heapfile:
  //    - this->datapage==NULL, but this->datapageId valid
  //    - this->datapageRid valid
  //    - current data page unpinned !!!
  // (3) if somewhere in heapfile
  //    - this->datapageId, this->datapage, this->datapageRid valid
  //    - current data page pinned
  // (4)- if the scan had already been done,
  //        dirpage = NULL;  datapageId = INVALID_PAGE
  
    if ((dirpage == NULL) && (datapageId == INVALID_PAGE))
        return DONE;

    if (datapage == NULL) {
        if (datapageId == INVALID_PAGE) {
            // heapfile is empty to begin with
            st = MINIBASE_BM->unpinPage(dirpageId);
            dirpage = NULL;
            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
            return DONE;

        } else {

            // pin first data page

#ifdef MULTIUSER
            _scan_pgid = datapageId;
            st = MINIBASE_LOCKMGR->lock_page(_scan_pgid,Shared);
            if (st != OK)  
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
#endif

            st = MINIBASE_BM->pinPage(datapageId, (Page *&) datapage);
            if (st != OK)
                return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
           
            // find the first record
            st = datapage->firstRecord(userrid);

            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
      
            return OK;
        }
    }
  
  // ASSERTIONS:
  // - this->datapage, this->datapageId, this->datapageRid valid
  // - current datapage pinned

    // unpin the current datapage
    st = MINIBASE_BM->unpinPage(datapageId);
    datapage = NULL;
    if (st != OK)
        return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
    
    // read next datapagerecord from current directory page
    // dirpage is set to NULL at the end of scan. Hence
  
    if (dirpage == NULL) {
        return DONE;
    }
  
    nextDataPageStatus = dirpage->nextRecord(datapageRid, datapageRid);
  
    if (nextDataPageStatus != OK) {    
        // we have read all datapage records on the current directory page
  
        // get next directory page
        nextDirPageId = dirpage->getNextPage();
  
        // unpin the current directory page
        st = MINIBASE_BM->unpinPage(dirpageId);
        dirpage = NULL;

        datapageId = INVALID_PAGE;
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
  
        if (nextDirPageId == INVALID_PAGE)
            return DONE;
        else {
          // ASSERTION:
          // - nextDirPageId has correct id of the page which is to get
  
            dirpageId = nextDirPageId;
  
#ifdef MULTIUSER
            _scan_pgid = dirpageId;
            st = MINIBASE_LOCKMGR->lock_page(_scan_pgid,Shared);
            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
#endif
            Status st = MINIBASE_BM->pinPage(dirpageId, (Page *&) dirpage);
            if (st != OK)
                return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
        
            if (dirpage == NULL)
                return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
  
            nextDataPageStatus = dirpage->firstRecord(datapageRid);

            if (nextDataPageStatus != OK)
                return  MINIBASE_CHAIN_ERROR( HEAPFILE, nextDataPageStatus );
        }
    }
    
    // ASSERTION:
    // - this->dirpageId, this->dirpage valid
    // - this->dirpage pinned
    // - the new datapage to be read is on dirpage
    // - this->datapageRid has the Rid of the next datapage to be read
    // - this->datapage, this->datapageId invalid
  
    // data page is not yet loaded: read its record from the directory page
    st = dirpage->getRecord(datapageRid, (char *) &dpinfo, dpinfoLen);
    if (st != OK)
        return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
    if (dpinfoLen != sizeof(DataPageInfo))
        return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
  
    // now get and pin the datapage
    datapageId = dpinfo.pageId;
  
#ifdef MULTIUSER
    _scan_pgid = datapageId;
    st = MINIBASE_LOCKMGR->lock_page(_scan_pgid,Shared);
    if (st != OK)
        return MINIBASE_CHAIN_ERROR( HEAPFILE, st);
#endif
  
    st = MINIBASE_BM->pinPage(dpinfo.pageId, (Page *&) datapage);
    if (st != OK)
        return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
    
    // - directory page is pinned
    // - datapage is pinned
    // - this->dirpageId, this->dirpage correct
    // - this->datapageId, this->datapage, this->datapageRid correct

    nxtUserStatus = datapage->firstRecord(userrid);
    if (nxtUserStatus != OK)
        return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
  
    return OK;
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char* recPtr, int& recLen)
{
    Status st;

    if (nxtUserStatus != OK) {
        nextDataPage();
    }

    if (datapage == NULL)
        return DONE;

    rid = userrid;     
    st  = datapage->getRecord(rid, recPtr, recLen);

    if (st != OK)
        return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

    nxtUserStatus = datapage->nextRecord(rid, userrid);

    return st;
}

// *******************************************
// Position the scan cursor to the record with the given rid.
// Returns OK if successful, non-OK otherwise.
Status Scan::position(RID rid)
{
    Status st;
    RID    nxtrid;

    //    st = peekNext(nxtrid);

    //if (nxtrid == rid)
    //  return OK;

    // This is kinda lame, but otherwise it will take all day.
    PageId pgid = rid.pageNo;
 

    if (datapageId != pgid) {

        // reset everything and start over from the beginning

        reset();
        st =  firstDataPage();

        if (st != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, st);


        while (datapageId != pgid) {
            st = nextDataPage();
            if (st != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, st);
        }
    }


    // Now we are on the correct page.

    st = datapage->firstRecord(userrid);
    if (st != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, st);

    st = peekNext(nxtrid);

    while ((st == OK) && (nxtrid != rid))
        st = mvNext(nxtrid);

    return st;
}

// *******************************************
// Move to the next record in a sequential scan.
// Also returns the RID of the (new) current record.
Status Scan::mvNext(RID& rid)
{
    Status st = OK;

    if (datapage == NULL)
        return DONE;

    nxtUserStatus = datapage->nextRecord(userrid, rid);

    if (nxtUserStatus == OK) {
        userrid = rid;               // save rid
        return st;
    } else {
        st = nextDataPage();
        if (st == OK)
            rid = userrid;
    }
  
    return st;
}

// *******************************************
