#include "Schema.h"

#include <cmath>
#include <cstring>


int Schema::openRel(char relName[ATTR_SIZE]) {
    int ret = OpenRelTable::openRel(relName);

    if (ret >= 0)
        return SUCCESS;

    return ret;
}


int Schema::closeRel(char relName[ATTR_SIZE]) {
    if ((strcmp(relName, RELCAT_RELNAME) == 0) || (strcmp(relName, ATTRCAT_RELNAME)) == 0)
        return E_NOTPERMITTED;

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    return OpenRelTable::closeRel(relId);
}