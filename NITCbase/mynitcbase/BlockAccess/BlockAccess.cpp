#include "BlockAccess.h"
#include <cstring>

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
    int ret = RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    if (ret != SUCCESS)
        return ret;

    int blockNum = relCatBuf.firstBlk;

    RecId recId = {-1, -1};

    int numSlots = relCatBuf.numSlotsPerBlk;
    int numAttrs = relCatBuf.numAttrs;

    int prevBlockNum = -1;

    while (blockNum != -1) {
        RecBuffer currentBlock(blockNum);

        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        unsigned char slotMap[numSlots];
        currentBlock.getSlotMap(slotMap);
        
        int freeSlot = -1;
        for (int i = 0; i < numSlots; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                freeSlot = i;
                break; 
            }
        }

        if (freeSlot != -1) {
            recId.block = blockNum;
            recId.slot = freeSlot;
            break;
        }

        prevBlockNum = blockNum;
        blockNum = currentHeader.rblock;
    }

    if (recId.block == -1 || recId.slot == -1) {
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;
        
        RecBuffer newBlock;

        int newBlockNum = newBlock.getBlockNum();

        if (newBlockNum == E_DISKFULL)
            return E_DISKFULL;

        recId.block = newBlockNum;
        recId.slot = 0;

        HeadInfo newBlockHeader;
        newBlock.getHeader(&newBlockHeader);
        newBlockHeader.lblock = prevBlockNum;
        newBlockHeader.numAttrs = numAttrs;
        newBlockHeader.numSlots = numSlots;
        newBlock.setHeader(&newBlockHeader);

        unsigned char newBlockSlotMap[numSlots];
        newBlock.getSlotMap(newBlockSlotMap);
        for (int i = 0; i < numSlots; i++)
            newBlockSlotMap[i] = SLOT_UNOCCUPIED;
        newBlock.setSlotMap(newBlockSlotMap);

        if (prevBlockNum != -1) {
            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevBlockHeader;
            prevBlock.getHeader(&prevBlockHeader);
            prevBlockHeader.rblock = recId.block;
            prevBlock.setHeader(&prevBlockHeader);
        }
        else {
            relCatBuf.firstBlk = recId.block;
            relCatBuf.lastBlk = recId.block;
            RelCacheTable::setRelCatEntry(relId, &relCatBuf);
        }
    }

    RecBuffer blockToInsert(recId.block);
    blockToInsert.setRecord(record, recId.slot);

    unsigned char slotMapToInsert[numSlots];
    blockToInsert.getSlotMap(slotMapToInsert);
    slotMapToInsert[recId.slot] = SLOT_OCCUPIED;
    blockToInsert.setSlotMap(slotMapToInsert);

    HeadInfo headerToInsert;
    blockToInsert.getHeader(&headerToInsert);
    headerToInsert.numEntries++;
    blockToInsert.setHeader(&headerToInsert);

    relCatBuf.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatBuf);

    int flag = SUCCESS;
    for (int attrOffset = 0; attrOffset < numAttrs; attrOffset++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatBuf);
        if (attrCatBuf.rootBlock == -1)
            continue;
        
        int ret = BPlusTree::bPlusInsert(relId, attrCatBuf.attrName, record[attrOffset], recId);
        if (ret == E_DISKFULL)
            flag = E_INDEX_BLOCKS_RELEASED;
    }

    return flag;
}


int BlockAccess::search(int relId, Attribute* record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    RecId recId;

    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    if (ret != SUCCESS)
        return ret;

    int rootBlock = attrCatBuf.rootBlock;

    if (rootBlock == -1)
        recId = linearSearch(relId, attrName, attrVal, op);
    else
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);

    if (recId.block == -1 || recId.slot == -1)
        return E_NOTFOUND;

    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    if (
        strcmp(relName, (char*)RELCAT_RELNAME) == 0 ||
        strcmp(relName, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttribute;
    strcpy(relNameAttribute.sVal, relName);

    RecId recId = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    if (recId.block == -1 || recId.slot == -1)
        return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(relCatEntryRecord, recId.slot);


    int currentBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;

    while(currentBlock != -1) {
        RecBuffer currentBlockBuffer(currentBlock);
        HeadInfo currentBlockHeader;
        currentBlockBuffer.getHeader(&currentBlockHeader);

        int nextBlock = currentBlockHeader.rblock;

        currentBlockBuffer.releaseBlock();
        currentBlock = nextBlock;
    }

    int numAttrsDeleted = 0;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    while(true) {
        RecId attrCatRecId = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        if (attrCatRecId.slot == -1 || attrCatRecId.block == -1)
            break;

        numAttrsDeleted++;

        RecBuffer currentBlock(attrCatRecId.block);

        HeadInfo currentBlockHeader;
        currentBlock.getHeader(&currentBlockHeader);

        Attribute record[ATTRCAT_NO_ATTRS];
        currentBlock.getRecord(record, attrCatRecId.slot);

        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        unsigned char slotMap[currentBlockHeader.numSlots];
        
        currentBlock.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        currentBlock.setSlotMap(slotMap);


        currentBlockHeader.numEntries--;
        currentBlock.setHeader(&currentBlockHeader);

        if (currentBlockHeader.numEntries == 0) {
            int leftBlock = currentBlockHeader.lblock;
            int rightBlock = currentBlockHeader.rblock;
            
            if (leftBlock != -1) {
                RecBuffer prevBlock(leftBlock);
                HeadInfo prevBlockHeader;

                prevBlock.getHeader(&prevBlockHeader);
                prevBlockHeader.rblock = rightBlock;
                prevBlock.setHeader(&prevBlockHeader);
            }

            if (rightBlock != -1) {
                RecBuffer nextBlock(rightBlock);
                HeadInfo nextBlockHeader;

                nextBlock.getHeader(&nextBlockHeader);
                nextBlockHeader.lblock = leftBlock;
                nextBlock.setHeader(&nextBlockHeader);
            }

            currentBlock.releaseBlock();
        }


        // condition to handle b+ trees
        if (rootBlock != -1)
            BPlusTree::bPlusDestroy(rootBlock);
    }

    HeadInfo relCatHeader;
    recBuffer.getHeader(&relCatHeader);

    unsigned char recSlotMap[relCatHeader.numSlots];

    recBuffer.getSlotMap(recSlotMap);
    recSlotMap[recId.slot] = SLOT_UNOCCUPIED;
    recBuffer.setSlotMap(recSlotMap);

    relCatHeader.numEntries--;
    recBuffer.setHeader(&relCatHeader);

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuf);
    relCatBuf.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatBuf);

    RelCatEntry attrCatBuf;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatBuf);
    attrCatBuf.numRecs -= numAttrsDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatBuf);

    return SUCCESS;

}

int BlockAccess::project(int relId, Attribute* record) {
    int block, slot;
    RecId prevRecId;

    RelCacheTable::getSearchIndex(relId, &prevRecId);

    if (prevRecId.block == -1 && prevRecId.slot == -1) {
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else {
        block = prevRecId.block;
        slot = prevRecId.slot+1;
    }

    while (block != -1) {
        RecBuffer currentBlock(block);

        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        unsigned char currentSlotMap[currentHeader.numSlots];
        currentBlock.getSlotMap(currentSlotMap);

        if (slot >= currentHeader.numSlots) {
            block = currentHeader.rblock;
            slot = 0;
        }
        else if (currentSlotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
        }
        else {
            break;
        }
    }

    if (block == -1)
        return E_NOTFOUND;

    RecId nextRecId = {block, slot};
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    RecBuffer targetBlock(block);
    targetBlock.getRecord(record, slot);

    return SUCCESS;
}