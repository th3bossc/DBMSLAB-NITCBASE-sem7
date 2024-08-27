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
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

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

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attrTypes);
    if (ret != SUCCESS)
        return ret;

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

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]) {
    int relId1 = OpenRelTable::getRelId(srcRelation1);
    int relId2 = OpenRelTable::getRelId(srcRelation2);

    if (relId1 == E_RELNOTOPEN || relId2 == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    AttrCatEntry attrCatEntry1, attrCatEntry2;
    int ret = AttrCacheTable::getAttrCatEntry(relId1, attribute1, &attrCatEntry1);
    if (ret != SUCCESS)
        return ret;
    ret = AttrCacheTable::getAttrCatEntry(relId2, attribute2, &attrCatEntry2);
    if (ret != SUCCESS)
        return ret;

    if (attrCatEntry1.attrType != attrCatEntry2.attrType)
        return E_ATTRTYPEMISMATCH;

    RelCatEntry relCatEntry1, relCatEntry2;
    ret = RelCacheTable::getRelCatEntry(relId1, &relCatEntry1);
    if (ret != SUCCESS)
        return ret;
    ret = RelCacheTable::getRelCatEntry(relId2, &relCatEntry2);
    if (ret != SUCCESS)
        return ret;

    int numAttrs1 = relCatEntry1.numAttrs;
    int numAttrs2 = relCatEntry2.numAttrs;

    for (int i = 0; i < numAttrs1; i++) {
        AttrCatEntry attrCatBuf1;
        AttrCacheTable::getAttrCatEntry(relId1, i, &attrCatBuf1);

        for (int j = 0; j < numAttrs2; j++) {
            AttrCatEntry attrCatBuf2;
            AttrCacheTable::getAttrCatEntry(relId2, j, &attrCatBuf2);

            if (i == attrCatEntry1.offset && j == attrCatEntry2.offset)
                continue;

            if (strcmp(attrCatBuf1.attrName, attrCatBuf2.attrName) == 0)
                return E_DUPLICATEATTR;
        }
    }

    if (attrCatEntry2.rootBlock == -1) {
        int ret = BPlusTree::bPlusCreate(relId2, attribute2);
        if (ret != SUCCESS)
            return ret;

    }

    int numAttrsInTarget = numAttrs1+numAttrs2-1;

    char targetRelAttrNames[numAttrsInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numAttrsInTarget];
    int targetAttrOffsetMap[numAttrsInTarget];
    
    int iter = 0;
    for (int i = 0; i < numAttrs1; i++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId1, i, &attrCatBuf);

        strcpy(targetRelAttrNames[iter], attrCatBuf.attrName);
        targetRelAttrTypes[iter] = attrCatBuf.attrType;

        iter++;
    }
    for (int i = 0; i < numAttrs2; i++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId2, i, &attrCatBuf);

        if (attrCatBuf.offset == attrCatEntry2.offset)
            continue;

        strcpy(targetRelAttrNames[iter], attrCatBuf.attrName);
        targetRelAttrTypes[iter] = attrCatBuf.attrType;

        iter++;
    }


    ret = Schema::createRel(targetRelation, numAttrsInTarget, targetRelAttrNames, targetRelAttrTypes);
    if (ret != SUCCESS)
        return ret;

    int newRelId = OpenRelTable::openRel(targetRelation);
    if (newRelId < 0 || newRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRelation);
        return newRelId;
    }

    Attribute record1[numAttrs1], record2[numAttrs2], targetRecord[numAttrsInTarget];



    RelCacheTable::resetSearchIndex(relId1);
    AttrCacheTable::resetSearchIndex(relId1, attribute1);
    while (BlockAccess::project(relId1, record1) == SUCCESS) {
        RelCacheTable::resetSearchIndex(relId2);
        AttrCacheTable::resetSearchIndex(relId2, attribute2);

        while (BlockAccess::search(relId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS) {
            
            iter = 0;
            for (int i = 0; i < numAttrs1; i++)
                memcpy(&targetRecord[iter++], &record1[i], sizeof(Attribute));

            for (int i = 0; i < numAttrs2; i++) {
                if (i == attrCatEntry2.offset)
                    continue;

                memcpy(&targetRecord[iter++], &record2[i], sizeof(Attribute));
            }

            int ret = BlockAccess::insert(newRelId, targetRecord);
            if (ret != SUCCESS) {
                OpenRelTable::closeRel(newRelId);
                Schema::deleteRel(targetRelation);
                return ret;
            }
        }
    } 

    OpenRelTable::closeRel(newRelId);

    return SUCCESS;
}