#include "BlockAccess.h"
#include <cstring>
#include <stdio.h>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);
    int block, slot;
    if (prevRecId.block == -1 && prevRecId.slot == -1) {
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);

        block = relCatBuf.firstBlk;
        slot = 0;
    }
    else {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }
    while (block != -1) {
        RecBuffer recBuffer(block);

        HeadInfo header;
        recBuffer.getHeader(&header);
        
        Attribute record[header.numAttrs];
        recBuffer.getRecord(record, slot);
        
        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);


        if (slot >= header.numSlots) {
            block = header.rblock;
            slot = 0;
            continue;
        }

        if (slotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
            continue;
        }

        AttrCatEntry attrCatBuf;
        int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

        int cmpVal = compareAttrs(record[attrCatBuf.offset], attrVal, attrCatBuf.attrType);

        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            RecId searchIndex = {block, slot};
            RelCacheTable::setSearchIndex(relId, &searchIndex);
            return searchIndex;
        }
        
        slot++;
    }

    return RecId({-1, -1});
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    memcpy(newRelationName.sVal, newName, ATTR_SIZE);
    Attribute oldRelationName;
    memcpy(oldRelationName.sVal, oldName, ATTR_SIZE);

    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, newRelationName, EQ);

    if (recId.block != -1 || recId.slot != -1)
        return E_RELEXIST;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);


    recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, oldRelationName, EQ);

    if (recId.block == -1 && recId.slot == -1)
        return E_RELNOTEXIST;

    RecBuffer recBuffer(recId.block);

    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, recId.slot);

    memcpy(&record[RELCAT_REL_NAME_INDEX], &newRelationName, ATTR_SIZE);

    recBuffer.setRecord(record, recId.slot);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while (true) {
        RecId attrEntryId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

        if (attrEntryId.block == -1 && attrEntryId.slot == -1)
            break;

        RecBuffer attrCatRecBuffer(attrEntryId.block);

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatRecBuffer.getRecord(attrCatRecord, attrEntryId.slot);

        memcpy(&attrCatRecord[ATTRCAT_REL_NAME_INDEX], &newRelationName, ATTR_SIZE);

        attrCatRecBuffer.setRecord(attrCatRecord, attrEntryId.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    memcpy(relNameAttr.sVal, relName, ATTR_SIZE);

    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    if (recId.block == -1 && recId.slot == -1) 
        return E_RELNOTEXIST;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID); 


    RecId attrToRenameId = {-1, -1};
    while(true) {
        RecId attrRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        if (attrRecId.block == -1 && attrRecId.slot == -1)
            break;

        RecBuffer recBuffer(attrRecId.block);
        Attribute record[ATTRCAT_NO_ATTRS];
        recBuffer.getRecord(record, attrRecId.slot);

        char attrName[ATTR_SIZE];
        memcpy(attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal, ATTR_SIZE);

        if (strcmp(attrName, oldName) == 0)
            attrToRenameId = attrRecId;

        if (strcmp(attrName, newName) == 0)
            return E_ATTREXIST;
    }

    if (attrToRenameId.block == -1 && attrToRenameId.slot == -1)
        return E_ATTRNOTEXIST;

    RecBuffer bufferToRename(attrToRenameId.block);
    Attribute recordToRename[ATTRCAT_NO_ATTRS];

    bufferToRename.getRecord(recordToRename, attrToRenameId.slot);

    memcpy(recordToRename[ATTRCAT_ATTR_NAME_INDEX].sVal, newName, ATTR_SIZE);

    bufferToRename.setRecord(recordToRename, attrToRenameId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute* record) {
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    int blockNum = relCatBuf.firstBlk;

    RecId newRecLocation = {-1, -1};

    int numSlots = relCatBuf.numSlotsPerBlk;
    int numAttrs = relCatBuf.numAttrs;

    int prevBlockNum = -1;

    while(blockNum != -1) {
        RecBuffer currentBlock(blockNum);

        HeadInfo header;
        currentBlock.getHeader(&header);

        unsigned char* slotMap;
        currentBlock.getSlotMap(slotMap);


        RecId freeLocationInCurrentBlock = {-1, -1};
        for (int i = 0; i < numSlots; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                freeLocationInCurrentBlock.block = blockNum;
                freeLocationInCurrentBlock.slot = i;
                break;
            }
        }

        if (freeLocationInCurrentBlock.block != -1 || freeLocationInCurrentBlock.slot != -1) {
            newRecLocation = freeLocationInCurrentBlock;
            break;
        }

        prevBlockNum = blockNum;
        blockNum = header.rblock;
    }


    if (newRecLocation.block == -1 && newRecLocation.slot == -1) {
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;

        RecBuffer newRecBlock;
        int newRecBlockNum = newRecBlock.getBlockNum();

        if (newRecBlockNum == E_DISKFULL)   
            return E_DISKFULL;

        newRecLocation.block = newRecBlockNum;
        newRecLocation.slot = 0;

        HeadInfo newBlockHeader;
        newBlockHeader.blockType = REC;
        newBlockHeader.pblock = -1;
        newBlockHeader.lblock = prevBlockNum;
        newBlockHeader.rblock = -1;
        newBlockHeader.numEntries = 0;
        newBlockHeader.numSlots = numSlots;
        newBlockHeader.numAttrs = numAttrs;


        newRecBlock.setHeader(&newBlockHeader);

        unsigned char newBlockSlotMap[numSlots];
        for (int i = 0; i < numSlots; i++)
            newBlockSlotMap[i] = SLOT_UNOCCUPIED;

        if (prevBlockNum != -1) {
            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevBlockHeader;
            prevBlock.getHeader(&prevBlockHeader);

            prevBlockHeader.rblock = newRecBlockNum;

            prevBlock.setHeader(&prevBlockHeader);
        }
        else {
            relCatBuf.firstBlk = newRecBlockNum;
            // RelCacheTable::setRelCatEntry(relId, &relCatBuf);
        }

        relCatBuf.lastBlk = newRecBlockNum;
        RelCacheTable::setRelCatEntry(relId, &relCatBuf);
    }

    RecBuffer currentBlock(newRecLocation.block);
    currentBlock.setRecord(record, newRecLocation.slot);

    unsigned char newRecSlotMap[numSlots];
    currentBlock.getSlotMap(newRecSlotMap);

    newRecSlotMap[newRecLocation.slot] = SLOT_OCCUPIED;
    currentBlock.setSlotMap(newRecSlotMap);

    HeadInfo newRecHeader;
    currentBlock.getHeader(&newRecHeader);
    newRecHeader.numEntries++;
    currentBlock.setHeader(&newRecHeader);

    relCatBuf.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatBuf);

    return SUCCESS;
}