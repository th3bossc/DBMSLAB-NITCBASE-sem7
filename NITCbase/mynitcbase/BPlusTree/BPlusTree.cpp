#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    if (attrCatBuf.rootBlock != -1)
        return SUCCESS;

    IndLeaf rootBlockBuf;
    int rootBlock = rootBlockBuf.getBlockNum();

    if (rootBlock == E_DISKFULL)
        return E_DISKFULL;

    attrCatBuf.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;
    while(block != -1) {
        RecBuffer currentBlock(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        currentBlock.getSlotMap(slotMap);

        for (int i = 0; i < relCatEntry.numSlotsPerBlk; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED)
                continue;

            Attribute record[relCatEntry.numAttrs];
            currentBlock.getRecord(record, i);

            RecId recId = {block, i};

            int ret = BPlusTree::bPlusInsert(relId, attrName, record[i], recId);
            if (ret == E_DISKFULL)
                return E_DISKFULL;
        }

        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        block = currentHeader.rblock;
    }

    return SUCCESS;
}

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    
    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    int block, index;

    if (searchIndex.block == -1 && searchIndex.index == -1) {
        block = attrCatBuf.rootBlock;
        index = 0;

        if (block == -1)
            return RecId({-1, -1});
    }
    else {
        block = searchIndex.block;
        index = searchIndex.index+1;

        IndLeaf leaf(block);

        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            block = leafHead.rblock;
            index = 0;

            if (block == -1)
                return RecId({-1, -1});
        }
    }

    
    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {
        IndInternal internalBlock(block);

        HeadInfo internalHead;
        internalBlock.getHeader(&internalHead);

        InternalEntry internalEntry;
        if (op == NE || op == LT || op == LE) {
            internalBlock.getEntry(&internalEntry, 0);
            block = internalEntry.lChild;
        }
        else {
            int targetIndex = -1;
            for (int i = 0; i < internalHead.numEntries; i++) {
                internalBlock.getEntry(&internalEntry, i);
                
                int cmpVal = compareAttrs(internalEntry.attrVal, attrVal, attrCatBuf.attrType);
                if (
                    (op == GT && cmpVal > 0) ||
                    ((op == EQ || op == GE) && cmpVal >= 0) 
                ) {
                    targetIndex = i;
                    break;
                }
            }

            if (targetIndex != -1) {
                internalBlock.getEntry(&internalEntry, targetIndex);
                block = internalEntry.lChild;
            }
            else {
                internalBlock.getEntry(&internalEntry, internalHead.numEntries-1);
                block = internalEntry.rChild;
            }
        }
    }

    while (block != -1) {
        IndLeaf leafBlock(block);
        
        HeadInfo leafHead;
        leafBlock.getHeader(&leafHead);

        Index leafEntry;
        while(index < leafHead.numEntries) {
            leafBlock.getEntry(&leafEntry, index);

            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatBuf.attrType);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                IndexId newSearchIndex = {block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &newSearchIndex);
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                return RecId({-1, -1});
            }

            index++;
        }

        if (op != NE)
            break;

        block = leafHead.rblock;
        index = 0;
    }

    return RecId({-1, -1});
}


int BPlusTree::bPlusDestroy(int rootBlockNum) {
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF) {
        IndLeaf rootBlock(rootBlockNum);
        rootBlock.releaseBlock();

        return SUCCESS;
    }
    else if (type == IND_INTERNAL) {
        IndInternal rootBlock(rootBlockNum);

        HeadInfo rootHeader;
        rootBlock.getHeader(&rootHeader);

        InternalEntry intEntry;
        rootBlock.getEntry(&intEntry, 0);
        BPlusTree::bPlusDestroy(intEntry.lChild);

        for (int i = 1; i < rootHeader.numEntries; i++) {
            rootBlock.getEntry(&intEntry, i);
            BPlusTree::bPlusDestroy(intEntry.rChild);
        }

        rootBlock.releaseBlock();
        return SUCCESS;
    }
    else 
        return E_INVALIDBLOCK;
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    if (ret != SUCCESS)
        return ret;

    int blockNum = attrCatBuf.rootBlock;
    if (blockNum == -1)
        return E_NOINDEX;

    int leafBlockNum = findLeafToInsert(blockNum, attrVal, attrCatBuf.attrType);

    Index indexEntry;
    indexEntry.attrVal = attrVal;
    indexEntry.block = recId.block;
    indexEntry.slot = recId.slot;
    ret = insertIntoLeaf(relId, attrName, leafBlockNum, indexEntry);

    if (ret == E_DISKFULL)
        return E_DISKFULL;

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {
        IndInternal intBlock(blockNum);

        HeadInfo intHeader;
        intBlock.getHeader(&intHeader);

        InternalEntry intEntry;
        int targetEntry = -1;
        for (int i = 0; i < intHeader.numEntries; i++) {
            intBlock.getEntry(&intEntry, i);
            int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrType);

            if (cmpVal >= 0) {
                targetEntry = i;
                break;
            }
        }

        if (targetEntry != -1) {
            intBlock.getEntry(&intEntry, targetEntry);
            blockNum = intEntry.lChild;
        }
        else {
            intBlock.getEntry(&intEntry, intHeader.numEntries-1);
            blockNum = intEntry.rChild;
        }
    }

    return blockNum;
}


int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    IndLeaf leafBlock(blockNum);

    HeadInfo leafHeader;
    leafBlock.getHeader(&leafHeader);

    Index indices[leafHeader.numEntries + 1];

    Index leafEntry;

    int iter = 0;
    int index = 0;
    for (iter = 0; iter < leafHeader.numEntries; iter++) {
        leafBlock.getEntry(&leafEntry, iter);
        if (compareAttrs(leafEntry.attrVal, indexEntry.attrVal, attrCatBuf.attrType) >= 0)
            break;
        indices[index++] = leafEntry;
    }
    indices[index++] = indexEntry;
    for (;iter < leafHeader.numEntries; iter++) {
        leafBlock.getEntry(&leafEntry, iter);
        indices[index++] = leafEntry;
    }

    if (leafHeader.numEntries != MAX_KEYS_LEAF) {
        leafHeader.numEntries++;
        leafBlock.setHeader(&leafHeader);

        for (int i = 0; i < leafHeader.numEntries; i++)
            leafBlock.setEntry(indices+i, i);

        return SUCCESS;
    }

    int newRightBlock = splitLeaf(blockNum, indices);
    if (newRightBlock == E_DISKFULL)
        return E_DISKFULL;

    if (leafHeader.pblock != -1) {
        InternalEntry intEntry;
        intEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        intEntry.lChild = blockNum;
        intEntry.rChild = newRightBlock;
        int ret = insertIntoInternal(relId, attrName, leafHeader.pblock, intEntry);
        if (ret != SUCCESS)
            return ret;
    }
    else {
        int ret = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlock);
        if (ret != SUCCESS)
            return ret;
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    IndLeaf rightBlock;
    IndLeaf leftBlock(leafBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = leafBlockNum;

    if (rightBlockNum == E_DISKFULL)
        return E_DISKFULL;

    HeadInfo leftHeader, rightHeader;
    leftBlock.getHeader(&leftHeader);
    rightBlock.getHeader(&rightHeader);

    rightHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    rightHeader.pblock = leftHeader.pblock;
    rightHeader.lblock = leftBlockNum;
    rightHeader.rblock = leftHeader.rblock;
    rightBlock.setHeader(&rightHeader);

    leftHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    leftHeader.rblock = rightBlockNum;
    leftBlock.setHeader(&leftHeader);

    for (int i = 0; i <= MIDDLE_INDEX_LEAF; i++)
        leftBlock.setEntry(&indices[i], i);

    for (int i = MIDDLE_INDEX_LEAF+1; i <= MAX_KEYS_LEAF; i++) 
        rightBlock.setEntry(&indices[i], i-MIDDLE_INDEX_LEAF-1);

    return rightBlockNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    IndInternal intBlock(intBlockNum);

    HeadInfo blockHeader;
    intBlock.getHeader(&blockHeader);

    InternalEntry entries[blockHeader.numEntries+1];
    int iter = 0;
    int index = 0;

    InternalEntry entry;
    for (iter = 0; iter < blockHeader.numEntries; iter++) {
        intBlock.getEntry(&entry, iter);

        if (compareAttrs(entry.attrVal, intEntry.attrVal, attrCatBuf.attrType) >= 0) {
            if (index > 0)
                entries[index-1].rChild = intEntry.lChild;
            break;
        }
        entries[index++] = entry;
    }
    entries[index++] = intEntry;
    if (iter < blockHeader.numEntries) {
        intBlock.getEntry(&entry, iter);
        entry.lChild = intEntry.rChild;
        entries[index++] = entry;
        iter++;
    }
    for (; iter < blockHeader.numEntries; iter++) {
        intBlock.getEntry(&entry, iter);
        entries[index++] = entry;
    }

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL) {
        blockHeader.numEntries++;
        intBlock.setHeader(&blockHeader);

        for (int i = 0; i < blockHeader.numEntries; i++) 
            intBlock.setEntry(&(entries[i]), i);
        
        return SUCCESS;
    }

    int newRightBlock = splitInternal(intBlockNum, entries);
    if (newRightBlock == E_DISKFULL) {
        BPlusTree::bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1) {
        InternalEntry newIntEntry;
        newIntEntry.attrVal = entries[MIDDLE_INDEX_INTERNAL].attrVal;
        newIntEntry.lChild = intBlockNum;
        newIntEntry.rChild = newRightBlock;
        return insertIntoInternal(relId, attrName, blockHeader.pblock, intEntry);
    }
    else {
        return createNewRoot(relId, attrName, entries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlock);
    }
    
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    IndInternal rightBlock;
    IndInternal leftBlock(intBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = intBlockNum;

    if (rightBlockNum == E_DISKFULL)
        return E_DISKFULL;

    HeadInfo leftHeader, rightHeader;
    leftBlock.getHeader(&leftHeader);
    rightBlock.getHeader(&rightHeader);

    rightHeader.numEntries = MAX_KEYS_INTERNAL/2;
    rightHeader.pblock = leftHeader.pblock;
    rightBlock.setHeader(&rightHeader);

    leftHeader.numEntries = MAX_KEYS_INTERNAL/2;
    leftBlock.setHeader(&leftHeader);

    for (int i = 0; i < MIDDLE_INDEX_INTERNAL; i++)
        leftBlock.setEntry(&internalEntries[i], i);
    for (int i = MIDDLE_INDEX_INTERNAL+1; i <= MAX_KEYS_INTERNAL; i++)
        rightBlock.setEntry(&internalEntries[i], i-MIDDLE_INDEX_INTERNAL-1);

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].rChild);


    BlockBuffer blockBuffer(internalEntries[MIDDLE_INDEX_INTERNAL+1].lChild);

    HeadInfo blockHeader;
    blockBuffer.getHeader(&blockHeader);
    blockHeader.pblock = rightBlockNum;
    blockBuffer.setHeader(&blockHeader);
    
    for (int i = 0; i < rightHeader.numEntries; i++) {
        BlockBuffer currentBlock(internalEntries[MIDDLE_INDEX_INTERNAL+i+1].rChild);
        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);
        currentHeader.pblock = rightBlockNum;
        currentBlock.setHeader(&currentHeader);
    }

    return rightBlockNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    IndInternal newRootBlock;

    int newRootBlockNum = newRootBlock.getBlockNum();
    if (newRootBlockNum == E_DISKFULL) {
        bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    HeadInfo newRootBlockHeader;
    newRootBlock.getHeader(&newRootBlockHeader);
    newRootBlockHeader.numEntries = 1;
    newRootBlock.setHeader(&newRootBlockHeader);

    InternalEntry internalEntry;
    internalEntry.attrVal = attrVal;
    internalEntry.lChild = lChild;
    internalEntry.rChild = rChild;
    newRootBlock.setEntry(&internalEntry, 0);

    BlockBuffer leftChild(lChild);
    BlockBuffer rightChild(rChild);

    HeadInfo leftChildHeader, rightChildHeader;
    leftChild.getHeader(&leftChildHeader);
    rightChild.getHeader(&rightChildHeader);
    leftChildHeader.pblock = newRootBlockNum;
    rightChildHeader.pblock = newRootBlockNum;
    leftChild.setHeader(&leftChildHeader);
    rightChild.setHeader(&rightChildHeader);

    attrCatBuf.rootBlock = newRootBlockNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    return SUCCESS;
}
