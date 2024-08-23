#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>
#include <stdio.h>


OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

AttrCacheEntry* createList(int length) {
    AttrCacheEntry* head = (AttrCacheEntry*) malloc(sizeof(AttrCacheEntry));
    AttrCacheEntry* tail = head;
    for (int i = 1; i < length; i++) {
        tail->next = (AttrCacheEntry*) malloc(sizeof(AttrCacheEntry));
        tail = tail->next;
    }
    tail->next = nullptr;
    return head;
}

void clearList(AttrCacheEntry* head) {
    for (AttrCacheEntry* it = head, *next; it != nullptr; it = next) {
        next = it->next;
        free(it);
    }
}

OpenRelTable::OpenRelTable() {
    for (int i = 0; i < MAX_OPEN; i++) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        OpenRelTable::tableMetaInfo[i].free = true;
    }

    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    for (int i = RELCAT_RELID; i <= ATTRCAT_RELID; i++) {
        relCatBlock.getRecord(relCatRecord, i);

        struct RelCacheEntry relCacheEntry;
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;

        RelCacheTable::relCache[i] = (struct RelCacheEntry*) malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
        tableMetaInfo[i].free = false;
        memcpy(tableMetaInfo[i].relName, relCacheEntry.relCatEntry.relName, ATTR_SIZE);
    };


    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    auto relCatListHead = createList(RELCAT_NO_ATTRS);
    auto attrCacheEntry = relCatListHead;

    for (int i = 0; i < RELCAT_NO_ATTRS; i++) {
        attrCatBlock.getRecord(attrCatRecord, i);

        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        (attrCacheEntry->recId).block = ATTRCAT_BLOCK;
        (attrCacheEntry->recId).slot = i;
        
        attrCacheEntry = attrCacheEntry->next;
    }
    AttrCacheTable::attrCache[RELCAT_RELID] = relCatListHead;

    auto attrCatListHead = createList(ATTRCAT_NO_ATTRS);
    attrCacheEntry = attrCatListHead;
    for (int i = RELCAT_NO_ATTRS; i < RELCAT_NO_ATTRS + ATTRCAT_NO_ATTRS; i++) {
        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        (attrCacheEntry->recId).block = ATTRCAT_BLOCK;
        (attrCacheEntry->recId).slot = i;

        attrCacheEntry = attrCacheEntry->next;
    }
    AttrCacheTable::attrCache[ATTRCAT_RELID] = attrCatListHead;


}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
    for (int i = 0; i < MAX_OPEN; i++) {
        if (!tableMetaInfo[i].free && strcmp(relName, tableMetaInfo[i].relName) == 0)
            return i;
    }

    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {
    for (int i = 2; i < MAX_OPEN; i++) {
        if (tableMetaInfo[i].free)
            return i;
    }

    return E_CACHEFULL;
}


int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
    int alreadyExists = OpenRelTable::getRelId(relName);

    if (alreadyExists >= 0)
        return alreadyExists;

    int freeSlot = OpenRelTable::getFreeOpenRelTableEntry();


    if (freeSlot == E_CACHEFULL)
        return E_CACHEFULL;

    Attribute relNameAttribute;
    memcpy(relNameAttribute.sVal, relName, ATTR_SIZE);
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    if (relcatRecId.block == -1 && relcatRecId.slot == -1)
        return E_RELNOTEXIST;
    RecBuffer recBuffer(relcatRecId.block);

    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, relcatRecId.slot);

    RelCatEntry relCatEntry;

    RelCacheTable::recordToRelCatEntry(record, &relCatEntry);

    RelCacheTable::relCache[freeSlot] = (RelCacheEntry*) malloc(sizeof(RelCacheEntry));

    RelCacheTable::relCache[freeSlot]->recId = relcatRecId;
    RelCacheTable::relCache[freeSlot]->relCatEntry = relCatEntry;


    int numAttrs = relCatEntry.numAttrs;
    AttrCacheEntry* listHead = createList(numAttrs);
    AttrCacheEntry* node = listHead;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    while(true) {
        RecId searchRes = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        if (searchRes.block != -1 && searchRes.slot != -1) {
            Attribute attrcatRecord[ATTRCAT_NO_ATTRS];

            RecBuffer attrRecBuffer(searchRes.block);

            attrRecBuffer.getRecord(attrcatRecord, searchRes.slot);

            AttrCatEntry attrCatEntry;
            AttrCacheTable::recordToAttrCatEntry(attrcatRecord, &attrCatEntry);

            node->recId = searchRes;
            node->attrCatEntry = attrCatEntry;
            node = node->next;
        }
        else 
            break;
    }

    AttrCacheTable::attrCache[freeSlot] = listHead;

    OpenRelTable::tableMetaInfo[freeSlot].free = false;
    memcpy(OpenRelTable::tableMetaInfo[freeSlot].relName, relCatEntry.relName, ATTR_SIZE);

    return freeSlot;

}

int OpenRelTable::closeRel(int relId) {
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (tableMetaInfo[relId].free == true)
        return E_RELNOTOPEN;


    RelCacheEntry* relCacheEntry = RelCacheTable::relCache[relId];

    if (relCacheEntry && relCacheEntry->dirty == true) {
        RecBuffer relCatBlock((relCacheEntry->recId).block);

        RelCatEntry relCatEntry = relCacheEntry->relCatEntry;
        Attribute record[RELCAT_NO_ATTRS];

        RelCacheTable::relCatEntryToRecord(&relCatEntry, record);

        relCatBlock.setRecord(record, (relCacheEntry->recId).slot);
    }

    free(relCacheEntry);
    RelCacheTable::relCache[relId] = nullptr;

    for (auto attrCacheEntry = AttrCacheTable::attrCache[relId]; attrCacheEntry != nullptr; attrCacheEntry = attrCacheEntry->next) {
        if (attrCacheEntry && attrCacheEntry->dirty == true) {
            RecBuffer attrCatBlock((attrCacheEntry->recId).block);

            AttrCatEntry attrCatEntry = attrCacheEntry->attrCatEntry;
            Attribute record[ATTRCAT_NO_ATTRS];

            AttrCacheTable::attrCatEntryToRecord(&attrCatEntry, record);

            attrCatBlock.setRecord(record, (attrCacheEntry->recId).slot);
        }
    }

    clearList(AttrCacheTable::attrCache[relId]);
    AttrCacheTable::attrCache[relId] = nullptr;

    tableMetaInfo[relId].free = true;
    return SUCCESS;
}

OpenRelTable::~OpenRelTable() {

    for (int i = 2; i < MAX_OPEN; i++) {
        if (!tableMetaInfo[i].free)
            OpenRelTable::closeRel(i);
    }



    for (int i = 2; i < MAX_OPEN; i++) {
        free(RelCacheTable::relCache[i]);
        clearList(AttrCacheTable::attrCache[i]);

        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }

    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty) {
        Attribute relCatRecord[RELCAT_NO_ATTRS];

        RelCatEntry relCatEntry = RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry;
        RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;

        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatRecord, recId.slot);

        free(RelCacheTable::relCache[ATTRCAT_RELID]);
    }

    if (RelCacheTable::relCache[RELCAT_RELID]->dirty) {
        Attribute relCatRecord[RELCAT_NO_ATTRS];

        RelCatEntry relCatEntry = RelCacheTable::relCache[RELCAT_RELID]->relCatEntry;
        RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;

        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatRecord, recId.slot);

        free(RelCacheTable::relCache[RELCAT_RELID]);
    }

    clearList(AttrCacheTable::attrCache[RELCAT_RELID]);
    clearList(AttrCacheTable::attrCache[ATTRCAT_RELID]);
}
