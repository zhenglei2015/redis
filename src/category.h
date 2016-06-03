#include "server.h"
#include "rio.h"

long long categoryObjectSize(robj *o);
void doCalculateCategory();
void ccCommand(client *c);
void addCateforyStats(robj *key, long long size, dict* d);

