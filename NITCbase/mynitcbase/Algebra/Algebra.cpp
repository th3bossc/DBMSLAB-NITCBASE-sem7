#include "Algebra.h"

#include <cstring>
#include <stdio.h>
#include <stdlib.h>

bool isNumber(char* str) {
    int len;
    float ignore;

    int ret = sscanf(str, " %f %n", &ignore, &len);

    return ret == 1 && len == strlen(str);
}


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

    int src_nAtrrs = relCatEntry.numAttrs;

    char attrNames[src_nAtrrs][ATTR_SIZE];
    int attrTypes[src_nAtrrs];

    for (int i = 0; i < src_nAtrrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        strcpy(attrNames[i], attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }

    ret = Schema::createRel(targetRel, src_nAtrrs, attrNames, attrTypes);

    if (ret != SUCCESS)
        return ret;

    int targetRelId = OpenRelTable::openRel(targetRel);

    if (targetRelId < 0 || targetRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[src_nAtrrs];
    while(BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) {
        int ret = BlockAccess::insert(targetRelId, record);
        if (ret != SUCCESS) {
            OpenRelTable::closeRel(targetRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    OpenRelTable::closeRel(targetRelId);
    return SUCCESS;
}



int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {
    if (
        strcmp(relName, (char*)RELCAT_RELNAME) == 0 ||
        strcmp(relName, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    if (relCatBuf.numAttrs != nAttrs)
        return E_NATTRMISMATCH;

    Attribute recordValues[nAttrs];


    for (int i = 0; i < nAttrs; i++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatBuf);

        if (attrCatBuf.attrType == NUMBER) {
            if (!isNumber(record[i]))
                return E_ATTRTYPEMISMATCH;
            recordValues[i].nVal = atof(record[i]);
        }
        else {
            strcpy(recordValues[i].sVal, record[i]);
        } 
    }

    return BlockAccess::insert(relId, recordValues);
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);

    if (srcRelId == E_RELNOTOPEN)
        return E_RELNOTOPEN;


    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int numAttrs = relCatEntry.numAttrs;

    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    for (int i = 0; i < numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        strcpy(attrNames[i], attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }

    int ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
    if (ret != SUCCESS)
        return ret;

    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0 || targetRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[numAttrs];

    while(BlockAccess::project(srcRelId, record) == SUCCESS) {
        int ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS) {
            OpenRelTable::closeRel(targetRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    return SUCCESS;
    
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);

    if (srcRelId == E_RELNOTOPEN)
        return srcRelId;

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int src_nAttrs = relCatEntry.numAttrs;

    int attrOffsets[tar_nAttrs];
    int attrTypes[tar_nAttrs];

    for (int i = 0; i < tar_nAttrs; i++) {
        AttrCatEntry attrCatEntry;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntry);

        if (ret != SUCCESS)
            return ret;

        attrOffsets[i] = attrCatEntry.offset;
        attrTypes[i] = attrCatEntry.attrType;
    }

    Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attrTypes);

    int tarRelId = OpenRelTable::openRel(targetRel);

    if (tarRelId < 0 || tarRelId >= MAX_OPEN)
        return tarRelId;

    Attribute record[src_nAttrs];


    RelCacheTable::resetSearchIndex(srcRelId);
    while(BlockAccess::project(srcRelId, record) == SUCCESS) {

        Attribute projRecord[tar_nAttrs];

        for (int i = 0; i < tar_nAttrs; i++)
            projRecord[i] = record[attrOffsets[i]];

        int ret = BlockAccess::insert(tarRelId, projRecord);

        if (ret != SUCCESS) {
            OpenRelTable::closeRel(tarRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);

    return SUCCESS;
}