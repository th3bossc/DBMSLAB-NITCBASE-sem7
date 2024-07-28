#include "Algebra.h"

#include <cstring>
#include <stdio.h>
#include <stdlib.h>

bool isNumber(char* str);


/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);

    if (srcRelId == E_RELNOTOPEN) 
        return srcRelId;

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry); // fix this


    if (ret == E_ATTRNOTEXIST)
        return ret;

    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER) {

        if (isNumber(strVal))
            attrVal.nVal = atof(strVal);
        else 
            return E_ATTRTYPEMISMATCH;
    }
    else if (type == STRING) {
        strcpy(attrVal.sVal, strVal);
    }


    RelCacheTable::resetSearchIndex(srcRelId);


    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    printf("|");
    for (int i = 0; i < relCatEntry.numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        printf(" %s\t|", attrCatEntry.attrName);
    }
    printf("\n");

    while(true) {
        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
        if (searchRes.block != -1 && searchRes.slot != -1) {
            Attribute record[relCatEntry.numAttrs];

            RecBuffer recBuf(searchRes.block);
            recBuf.getRecord(record, searchRes.slot);

            printf("|");
            for (int i = 0; i < relCatEntry.numAttrs; i++) {
                AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
                
                (attrCatEntry.attrType == NUMBER)
                    ? printf(" %f\t|", record[i].nVal)
                    : printf(" %s\t|", record[i].sVal);
            }
            printf("\n");
        }
        else {
            break;
        }
    }

    return SUCCESS;
}

bool isNumber(char* str) {
    int len;
    float ignore;

    int ret = sscanf(str, " %f %n", &ignore, &len);

    return ret == 1 && len == strlen(str);
}

