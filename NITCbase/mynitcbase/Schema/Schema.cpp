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

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
    if (    
        strcmp(oldRelName, (char*)RELCAT_RELNAME) == 0      || 
        strcmp(oldRelName, (char*)ATTRCAT_RELNAME) == 0     ||
        strcmp(newRelName, (char*)RELCAT_RELNAME) == 0      ||
        strcmp(newRelName, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(oldRelName);

    if (relId != E_RELNOTOPEN)
        return E_RELOPEN;

    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}

int Schema::renameAttr(char relname[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE]) {
    if (
        strcmp(relname, (char*)RELCAT_RELNAME) == 0      || 
        strcmp(relname, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relname);

    if (relId != E_RELNOTOPEN)
        return E_RELOPEN;

    int retVal = BlockAccess::renameAttribute(relname, oldAttrName, newAttrName);
    return retVal;
}


int Schema::createRel(char relName[], int numOfAttributes, char attrNames[][ATTR_SIZE], int attrType[]) {
    Attribute relNameAttribute;
    strcpy(relNameAttribute.sVal, relName);

    RecId targetRelId = {-1, -1};

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    targetRelId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    if (targetRelId.block != -1 || targetRelId.slot != -1)
        return E_RELEXIST;

    for (int i = 0; i < numOfAttributes; i++) {
        for (int j = i+1; j < numOfAttributes; j++) {
            if (strcmp(attrNames[i], attrNames[j]) == 0)
                return E_DUPLICATEATTR;
        }
    }

    Attribute relCatRecord[RELCAT_NO_ATTRS];

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = numOfAttributes;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor(2016 / (16*numOfAttributes + 1));

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);

    if (retVal != SUCCESS)
        return retVal;

    for (int i = 0; i < numOfAttributes; i++) {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrNames[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrType[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);

        if (retVal != SUCCESS) {
            Schema::deleteRel(relName);
            return E_DISKFULL;
        }
    }

    return SUCCESS;
}

int Schema::deleteRel(char relName[ATTR_SIZE]) {
    if (
        strcmp(relName, (char*)RELCAT_RELNAME) == 0 ||
        strcmp(relName, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId != E_RELNOTOPEN)
        return E_RELOPEN;

    return BlockAccess::deleteRelation(relName);
}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE]) {
    if (
        strcmp(relName, (char*) RELCAT_RELNAME) == 0    ||
        strcmp(relName, (char*) ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if (relId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE]) {
    if (
        strcmp(relName, (char*) RELCAT_RELNAME) == 0    ||
        strcmp(relName, (char*) ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if (relId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    if (ret == E_ATTRNOTEXIST)
        return E_ATTRNOTEXIST;

    int rootBlock = attrCatBuf.rootBlock;

    if (rootBlock == -1)
        return E_NOINDEX;

    BPlusTree::bPlusDestroy(rootBlock);

    attrCatBuf.rootBlock = -1;
    ret = AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    return ret;

}