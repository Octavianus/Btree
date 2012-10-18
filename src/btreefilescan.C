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

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::~BTreeFileScan ()
{
  // put your code here
}

int BTreeFileScan::keysize() 
{
  // put your code here
}

Status BTreeFileScan::get_next (RID & rid, void* keyptr)
{
  // put your code here
}

Status BTreeFileScan::delete_current ()
{
  // put your code here
}
