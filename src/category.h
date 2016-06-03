#include "server.h"
#include "rio.h"

ssize_t categoryObjectSize(robj *o);
void doCalculateCategory();
void ccCommand(client *c);
void addCateforyStats(robj *key, int size, dict* d);

