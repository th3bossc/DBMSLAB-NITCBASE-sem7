#include "AttrCacheTable.h"

#include <cstring>
#include <stdio.h>

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];


int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    if (relId < 0 || relId > MAX_OPEN)
        return E_OUTOFBOUND;

    for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
        if (entry->attrCatEntry.offset == attrOffset) {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS], AttrCatEntry* attrCatEntry) {
    strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
    strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
    attrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
    attrCatEntry->primaryFlag = (bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
    attrCatEntry->rootBlock = (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    attrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == nullptr)
        return E_RELNOTOPEN;

    AttrCacheEntry* attrCacheEntry = nullptr;
    for (auto iter = AttrCacheTable::attrCache[relId]; iter != nullptr; iter = iter->next) {
        if (strcmp(attrName, (iter->attrCatEntry).attrName) == 0) {
            attrCacheEntry = iter;
            break;
        }
    }

    if (attrCacheEntry == nullptr)
        return E_ATTRNOTEXIST;

    *attrCatBuf = attrCacheEntry->attrCatEntry;
    return SUCCESS;
}

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId* searchIndex) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == NULL)
        return E_RELNOTOPEN;


    for (auto attrCacheEntry = attrCache[relId]; attrCacheEntry != NULL; attrCacheEntry = attrCacheEntry->next) {
        if (strcmp(attrCacheEntry->attrCatEntry.attrName, attrName) == 0) {
            *searchIndex = attrCacheEntry->searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId* searchIndex) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == NULL)
        return E_RELNOTOPEN;


    for (auto attrCacheEntry = attrCache[relId]; attrCacheEntry != NULL; attrCacheEntry = attrCacheEntry->next) {
        if (attrCacheEntry->attrCatEntry.offset == attrOffset) {
            *searchIndex = attrCacheEntry->searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId* searchIndex) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == NULL)
        return E_RELNOTOPEN;


    for (auto attrCacheEntry = attrCache[relId]; attrCacheEntry != NULL; attrCacheEntry = attrCacheEntry->next) {
        if (strcmp(attrCacheEntry->attrCatEntry.attrName, attrName) == 0) {
            attrCacheEntry->searchIndex = *searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}


int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId* searchIndex) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == NULL)
        return E_RELNOTOPEN;


    for (auto attrCacheEntry = attrCache[relId]; attrCacheEntry != NULL; attrCacheEntry = attrCacheEntry->next) {
        if (attrCacheEntry->attrCatEntry.offset == attrOffset) {
            attrCacheEntry->searchIndex = *searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {
    IndexId indexId = {-1, -1};
    return setSearchIndex(relId, attrName, &indexId);
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset) {
    IndexId indexId = {-1, -1};
    return setSearchIndex(relId, attrOffset, &indexId);
}


int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == NULL)
        return E_RELNOTOPEN;

    for (AttrCacheEntry* it = attrCache[relId]; it != NULL; it = it->next) {
        AttrCatEntry currentEntry = it->attrCatEntry;

        if (strcmp(currentEntry.attrName, attrName) == 0) {
            it->attrCatEntry = *attrCatBuf;
            it->dirty = true;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (attrCache[relId] == NULL)
        return E_RELNOTOPEN;

    for (AttrCacheEntry* it = attrCache[relId]; it != NULL; it = it->next) {
        AttrCatEntry currentEntry = it->attrCatEntry;

        if (currentEntry.offset == attrOffset) {
            it->attrCatEntry = *attrCatBuf;
            it->dirty = true;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, union Attribute record[ATTRCAT_NO_ATTRS]) {
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = (int)attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = (int)attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = (int)attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = (int)attrCatEntry->offset;
}
