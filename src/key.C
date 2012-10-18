/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */

int keyCompare(const void *key1, const void *key2, AttrType t)
{
  Keytype *k1 = (Keytype *)key1;
  Keytype *k2 = (Keytype *)key2;

  switch(t) {

  case attrInteger:
    return k1->intkey - k2->intkey;
    break;

  case attrString:
    return strncmp(k1->charkey, k2->charkey, MAX_KEY_SIZE1);
    break;

  default:                        // sanity check
    assert(0);
    return 0;
  }

  return 1;
}

int get_key_length(const void *key, const AttrType key_type)
{
  int len;

  switch(key_type) {

  case attrInteger:
    return sizeof(int);
    break;

  case attrString:
    len = strlen((char *) key); 
    return len+1;
    break;

  default:
    assert(0);
    return 0;
  }
}
    
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
  int keylen = get_key_length(key, key_type);
  switch(ndtype) {
  case INDEX:
    return keylen + sizeof(PageId);
  case LEAF:
    return keylen + sizeof(RID);
  default:                        // sanity check
    assert(0);
  }
  assert(0);
  return 0;
}

 
/*
 * fill_entry_key: used by make_entry (below) to write the <key> part
 * of a KeyDataEntry (*target, that is). 
 * Returns <key> part length in *pentry_key_len.
 */ 

static void fill_entry_key(Keytype *target,
                           const void *key, AttrType key_type,
                           int *pentry_key_len)
{
  switch(key_type) {
  case attrInteger:
    {
      int *p = &target->intkey;
      *p = *(int *) key;
      *pentry_key_len = sizeof(*p);
      return;
    }
  case attrString: 
    {
      char *p = (char *) target;
      int len = strlen((char *)key)+1;
      if (len >= MAX_KEY_SIZE1)
        // XXX
        ;
      strcpy(p, (char *) key);
      *pentry_key_len = len;
      return;
    }
  default:                        // sanity check
    assert(0); 
  }
}

/*
 * fill_entry_data: writes <data> part to a KeyDataEntry (into which
 * `target' points).  Returns length of <data> part in *pentry_data_len.
 *
 * Note that these do memcpy's instead of direct assignments because 
 * `target' may not be properly aligned.
 */

static void fill_entry_data(char *target, 
                            Datatype source, nodetype ndtype,
                            int *pentry_data_len)
{
  switch(ndtype) {
  case INDEX:
    {
//      PageId *p = &target->pageNo;
      Datatype src = source;
      memcpy(target, &src, sizeof(PageId));
      *pentry_data_len = sizeof(PageId);
      return;
    }
  case LEAF:
    {
//      RID *p = &target->rid;
      Datatype src = source;
//          cout << " size " << sizeof(RID) << endl;
      memcpy((void*)target, (void*)(&source), sizeof(RID));      
      *pentry_data_len = sizeof(RID);
      return;
    }
  default:
    assert(0); // internal error
  }
}
    
/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */

void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
  int keylen, datalen;
  
  fill_entry_key(&target->key, key, key_type, &keylen);

  // below we can't say "&target->data" because <data> field may actually
  // start before that location (recall that KeyDataEntry is simply 
  // a chunk of memory big enough to hold any legal <key,data> pair).
  fill_entry_data((char *) (((char *)target) + keylen),
                  data, ndtype,  
                  &datalen);                
  *pentry_len = keylen + datalen;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */

void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
  int datalen;
  int keylen;
  
  switch(ndtype) {
  case INDEX:
    datalen = sizeof(PageId);
    break;
  case LEAF:
    datalen = sizeof(RID);
    break;
  default:
    cout << "Node type unknown: " << ndtype << endl;
    assert(0);
  }

  keylen = entry_len - datalen;
  if ( targetkey )
    memcpy(targetkey, psource, keylen);
  if ( targetdata )
    memcpy(targetdata, ((char*)psource) + keylen, datalen);
}
