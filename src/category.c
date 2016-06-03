#include "category.h"



/* Like rdbSaveStringObjectRaw() but handle encoded objects */
ssize_t sizeOfStringObject(robj *obj) {
    /* Avoid to decode the object, then encode it again, if the
     * object is already integer encoded. */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return sizeof(long long);
    } else {
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return sdsalloc(obj->ptr);
    }
}


/* Save a Redis object. Returns -1 on error, number of bytes written on success. */
ssize_t categoryObjectSize(robj *o){
    if (o->type == OBJ_STRING) {
        /* Save a string value */
        sizeOfStringObject(o);
    } else if (o->type == OBJ_LIST) {
        /* Save a list value */
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;
            ssize_t totalsize = 0;

            do {
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    size_t compress_len = quicklistGetLzf(node, &data);
                    totalsize += compress_len;
                } else {
                    totalsize += node->sz;
                }
            } while ((node = node->next));
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        /* Save a set value */
        if (o->encoding == OBJ_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;
            ssize_t totalsize = 0;
            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                totalsize += sizeOfStringObject(eleobj);
            }
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);
            return l;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
            return l;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;
            ssize_t totalsize = 0;
            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                totalsize += sizeOfStringObject(eleobj);
            }
            dictReleaseIterator(di);
            return totalsize;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        /* Save a hash value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
            return l;

        } else if (o->encoding == OBJ_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;
            ssize_t totalsize = 0;
            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);
                totalsize += sizeOfStringObject(key);
                totalsize += sizeOfStringObject(val);
            }
            dictReleaseIterator(di);
            return totalsize;
        } else {
            serverPanic("Unknown hash encoding");
        }

    } else {
        serverPanic("Unknown object type");
    }
    return -1;
}

void saveResult() {
    rio r;
    FILE fp;
}