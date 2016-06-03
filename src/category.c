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
    FILE *fp;
    char tmpfile[300];

    snprintf(tmpfile,256,"category-%d.txt", (int) getpid());
    fp = fopen(tmpfile, "w");
    rioInitWithFile(&r,fp);

    dictIterator *di = dictGetIterator(server.categoryStatsDict);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        sds val = dictGetVal(de);
        char line[1000];
        int totalLen = sdslen(key) + sdslen(val) + 3;
        snprintf(line, totalLen, "%s$$%s\n", key, val);
        rioWrite(&r, line, totalLen);
    }
    dictReleaseIterator(di);
    /* Make sure data will not remain on the OS's output buffers */
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;
    return;

    werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return ;
}

void emptyCategoryStatsDict() {
    dictEmpty(server.categoryStatsDict, NULL);
}

void doCalculateCategory() {
    emptyCategoryStatsDict();
    dictIterator *di = NULL;
    dictEntry *de;

    dict *d = server.db[0].dict;
    if (dictSize(d) == 0) return;
    di = dictGetSafeIterator(d);
    if (!di) return ;

    /* Iterate this DB writing every entry */
    while((de = dictNext(di)) != NULL) {
        sds keystr = dictGetKey(de);
        robj key, *o = dictGetVal(de);

        initStaticStringObject(key,keystr);
        int keysize = sdslen(keystr);
        ssize_t objsize = categoryObjectSize(o);
        addCateforyStats(&key, keysize + objsize);
    }
    dictReleaseIterator(di);
    saveResult();
}
void ccCommand(client *c) {
    if(fork() == 0) {
        doCalculateCategory();
        saveResult();
        exit(0);
    } else {
        char line[300];
        snprintf(line, 256, "%d is runing to do calculate", (int) getpid());
        addReplyStatus(c, line);
    }
}

