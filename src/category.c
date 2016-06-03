#include "category.h"

static char *filename = "category.txt";

ssize_t sizeOfStringObject(robj *obj) {
    if (obj->encoding == OBJ_ENCODING_INT) {
        return sizeof(long long);
    } else {
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return sdsalloc(obj->ptr);
    }
}

long long categoryObjectSize(robj *o){
    if (o->type == OBJ_STRING) {
        /* Save a string value */
        sizeOfStringObject(o);
    } else if (o->type == OBJ_LIST) {
        /* Save a list value */
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;
            long long totalsize = 0;
            do {
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    long long compress_len = quicklistGetLzf(node, &data);
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
            long long totalsize = 0;
            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                totalsize += sizeOfStringObject(eleobj);
            }
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            long long l = intsetBlobLen((intset*)o->ptr);
            return l;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            long long l = ziplistBlobLen((unsigned char*)o->ptr);
            return l;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;
            long long totalsize = 0;
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
            long long l = ziplistBlobLen((unsigned char*)o->ptr);
            return l;

        } else if (o->encoding == OBJ_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;
            long long totalsize = 0;
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

void saveResult(dict* tempDict) {
    rio r;
    FILE *fp;
    char tmpfile[300];
    snprintf(tmpfile,256,"category-%d.txt", (int) getpid());
    fp = fopen(tmpfile, "w");
    rioInitWithFile(&r,fp);

    dictIterator *di = dictGetIterator(tempDict);
    dictEntry *de;
    char *line;
    line = (char *)zmalloc(10000);
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        sds val = dictGetVal(de);
        int totalLen = sdslen(key) + sdslen(val);
        if(totalLen > 10000) {
            zfree(line);
            line = zmalloc(totalLen + 200);
        }
        snprintf(line, totalLen + 20, "%s$$%s\n", key, val);
        rioWrite(&r, line, totalLen + 3);
    }
    zfree(line);
    dictReleaseIterator(di);
    // flush 掉
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;
    if(rename(tmpfile,filename) == -1) {
            unlink(tmpfile);
            goto werr;
    }
    return;

    werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return ;
}

void doCalculateCategory() {
    dict *tempDict;
    tempDict = dictCreate(&categoryStatsDictType, NULL);
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
        long long objsize = categoryObjectSize(o);
        addCateforyStats(&key, keysize + objsize, tempDict);
    }
    dictReleaseIterator(di);
    saveResult(tempDict);
}

void categoryInfoInsert(void *p) {
    p = NULL; // 防止 warning
    FILE *file = fopen(filename, "r");
    rio r;
    rioInitWithFile(&r, file);
    char line[10000];
    int len = 10000;
    dictEmpty(server.categoryStatsDict, NULL);
    while(fgets(line,len,file)!=NULL) {
        int pos = strstr(line, "$$") - line;
        if(pos > 0) {
            sds key = sdsnewlen(line, pos);
            sds val = sdsnewlen(line + pos + 2, strlen(line) - pos - 3);
            if(dictAdd(server.categoryStatsDict, key, val) != DICT_OK) {

            } else {

            }
        }
    }
    fclose(file);
}

void *waitToUpdate(void *p) {
    pid_t calp = *(pid_t*)(p);
    long long ts = time(NULL);
    waitpid(calp,NULL,0);
    printf("category occupy memory space over pid %d\n", calp);
    dictEmpty(server.categoryStatsDict, NULL);
    categoryInfoInsert(0);
    long long te = time(NULL);
    printf("time used %lld\n", te - ts);
    server.calculateCategoryChild = -1;
    return ((void *)0);
}


void addCateforyStats(robj *key, long long valsize, dict* tempDict) {
    int len = strlen(key->ptr);
    char *categoryKey = (char *)sdsnewlen(key->ptr, len);
    for(int i = 0; i < len; i++) {
        if(categoryKey[i] == '.') {
            categoryKey[i] = '\0';
            break;
        }
    }
    sds k = sdsnewlen(categoryKey, strlen(categoryKey));// 这一步没必要
    long long change = valsize;
    char changeStr[50];
    dictEntry *di;
    if((di = dictFind(tempDict, k)) == NULL) {
        sprintf(changeStr, "%lld" , change);
        sds v = sdsnew(changeStr);
        dictAdd(tempDict, k, v);
    } else {
        sds* oldv = dictGetVal(di);
        long long old = atol((char *)oldv);
        sprintf(changeStr, "%lld" , change + old);
        sds v = sdsnew(changeStr);
        dictDelete(tempDict,k);
        dictAdd(tempDict, k, v);
    }
}

void ccCommand(client *c) {
    pid_t p;
    if(server.calculateCategoryChild != -1) {
        addReplyError(c, "ctegory calculating thread is running\n");
    } else if((p = fork()) == 0) {
        doCalculateCategory();
        exit(0);
    } else {
        server.calculateCategoryChild = p;
        printf("xxxxx %d\n", p);
        pthread_t tid;
        printf("CCCCCCCCCCC start %d \n", p);
        if (pthread_create(&tid,NULL,waitToUpdate,&p) == 0) {
            pthread_detach(tid);
            char line[300];
            snprintf(line, 256, "%d is runing to do calculate", (int) getpid());
            addReplyStatus(c, line);
        } else {
            addReplyStatus(c, "create wait thread failed");
        }
    }
}
