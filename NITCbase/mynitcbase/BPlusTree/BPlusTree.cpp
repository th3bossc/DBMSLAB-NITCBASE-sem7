#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    
    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int block, index;
    if (searchIndex.block == -1 || searchIndex.index == -1) {
        block = attrCatEntry.rootBlock;
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
    
        HeadInfo intHead;
        internalBlock.getHeader(&intHead);

        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE) {
            internalBlock.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else {
            int targetIndex = -1;
            for (int i = 0; i < intHead.numEntries; i++) {
                internalBlock.getEntry(&intEntry, i);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);

                if (cmpVal >= 0) {
                    targetIndex = i;
                    break;
                }
            }
    
            if (targetIndex == -1) {
                internalBlock.getEntry(&intEntry, intHead.numEntries-1);
                block = intEntry.rChild;
            }
            else {
                internalBlock.getEntry(&intEntry, targetIndex);
                block = intEntry.lChild;
            }
        }
    }

    while (block != -1) {
        IndLeaf leafBlock(block);

        HeadInfo leafHead;
        leafBlock.getHeader(&leafHead);

        Index leafEntry;
        while (index < leafHead.numEntries) {
            leafBlock.getEntry(&leafEntry, index);
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if (
                (op == EQ && cmpVal == 0)   ||
                (op == LE && cmpVal <= 0)   ||
                (op == LT && cmpVal < 0)    ||
                (op == GT && cmpVal > 0)    ||
                (op == GE && cmpVal >= 0)   ||
                (op == NE && cmpVal != 0)
            ) {
                searchIndex = {block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId({leafEntry.block, leafEntry.slot});
            }
            else if (
                (op == EQ || op == LE || op == LT)  &&
                cmpVal > 0
            ) {
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

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    if (ret != SUCCESS)
        return ret;

    IndLeaf rootBlockBuf;
    int rootBlock = rootBlockBuf.getBlockNum();
    printf("This is the rootblock number: %d\n", rootBlock);

    if (rootBlock == E_DISKFULL)   
        return E_DISKFULL;

    attrCatBuf.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    int numSlots = relCatBuf.numSlotsPerBlk;
    int numAttrs = relCatBuf.numAttrs;
    int attrOffset = attrCatBuf.offset;

    int block = relCatBuf.firstBlk;
    while(block != -1) {
        RecBuffer currentBlock(block);

        unsigned char slotMap[numSlots];
        currentBlock.getSlotMap(slotMap);


        for (int i = 0; i < numSlots; i++) {
            if (slotMap[i] == SLOT_OCCUPIED) {
                Attribute record[numAttrs];
                currentBlock.getRecord(record, i);

                RecId recId = {block, i};

                int ret = BPlusTree::bPlusInsert(relId, attrName, record[attrOffset], recId);
                if (ret != SUCCESS)
                    return ret;
            }
        }

        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        block = currentHeader.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF) {
        IndLeaf leafNode(rootBlockNum);
        leafNode.releaseBlock();

        return SUCCESS;
    }
    else if (type == IND_INTERNAL) {
        IndInternal internalNode(rootBlockNum);

        HeadInfo internalHeader;
        internalNode.getHeader(&internalHeader);

        int numEntries = internalHeader.numEntries;
        for (int i = 0; i < numEntries; i++) {
            InternalEntry currentEntry;
            internalNode.getEntry(&currentEntry, i);

            if (currentEntry.lChild != -1) {
                int ret = BPlusTree::bPlusDestroy(currentEntry.lChild);
                if (ret != SUCCESS)
                    return ret;
            }
            if (currentEntry.rChild != -1) {
                int ret = BPlusTree::bPlusDestroy(currentEntry.rChild);
                if (ret != SUCCESS)
                    return ret;
            }
        }

        internalNode.releaseBlock();

        return SUCCESS;
    }

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

    if (attrCatBuf.attrType == NUMBER)
        printf("inserted %lf\tblockNo: %d\n", attrVal.nVal, blockNum);
    else 
        printf("inserted %s\tblockNo: %d\n", attrVal.sVal, blockNum);

    int leafBlockNum = BPlusTree::findLeafToInsert(blockNum, attrVal, attrCatBuf.attrType);



    Index indexEntry;
    indexEntry.attrVal = attrVal;
    indexEntry.block = recId.block;
    indexEntry.slot = recId.slot;

    ret = BPlusTree::insertIntoLeaf(relId, attrName, leafBlockNum, indexEntry);

    if (ret == E_DISKFULL) {
        BPlusTree::bPlusDestroy(blockNum);
        attrCatBuf.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);
        return E_DISKFULL;
    }


    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    printf("test: %d\n", rootBlock);
    int blockNum = rootBlock;

    while(StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {
        IndInternal internalBlock(blockNum);

        HeadInfo internalHeader;
        internalBlock.getHeader(&internalHeader);

        int numEntries = internalHeader.numEntries;

        InternalEntry entryBuffer;
        int targetEntry = -1;
        for (int i = 0; i < numEntries; i++) {
            InternalEntry entryBuffer;
            internalBlock.getEntry(&entryBuffer, i);

            int cmpVal = compareAttrs(entryBuffer.attrVal, attrVal, attrType);
            if (cmpVal >= 0) {
                targetEntry = i;
                break;
            }
        }

        if (rootBlock == blockNum)
            printf("halehale %d, %d\n", targetEntry, numEntries);
        if (targetEntry == -1) {
            internalBlock.getEntry(&entryBuffer, numEntries-1);
            blockNum = entryBuffer.rChild;
        }
        else {
            internalBlock.getEntry(&entryBuffer, targetEntry);
            blockNum = entryBuffer.lChild;
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

    int numEntries = leafHeader.numEntries;
    int attrType = attrCatBuf.attrType;

    Index indices[numEntries + 1];

    int iter = 0;
    int index = 0;

    for (iter = 0; iter < numEntries; iter++) {
        Index currentIndex;
        leafBlock.getEntry(&currentIndex, iter++);

        int cmpVal = compareAttrs(currentIndex.attrVal, indexEntry.attrVal, attrType);
        if (cmpVal <= 0)
            indices[index++] = currentIndex;
        else 
            break;
    }
    
    indices[index++] = indexEntry;

    for (; iter < numEntries; iter++) {
        Index currentIndex;
        leafBlock.getEntry(&currentIndex, iter++);

        indices[index++] = currentIndex;
    }

    if (numEntries != MAX_KEYS_LEAF) {
        leafHeader.numEntries++;
        leafBlock.setHeader(&leafHeader);

        for (int i = 0; i <= numEntries; i++)
            leafBlock.setEntry(&indices[i], i);

        return SUCCESS;
    }

    int newRightBlock = splitLeaf(blockNum, indices);

    if (newRightBlock == E_DISKFULL)
        return E_DISKFULL;

    if (blockNum != attrCatBuf.rootBlock) {
        InternalEntry internalEntry;
        internalEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        internalEntry.lChild = blockNum;
        internalEntry.rChild = newRightBlock;

        int ret = BPlusTree::insertIntoInternal(relId, attrName, leafHeader.pblock, internalEntry);
        if (ret == E_DISKFULL)
            return E_DISKFULL;
    }
    else {
        int ret = BPlusTree::createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlock);
        if (ret == E_DISKFULL)
            return E_DISKFULL;
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


    HeadInfo rightBlockHeader, leftBlockHeader;

    leftBlock.getHeader(&leftBlockHeader);
    rightBlock.getHeader(&rightBlockHeader);

    rightBlockHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlockHeader.lblock = leftBlockNum;
    rightBlockHeader.rblock = leftBlockHeader.rblock;
    rightBlock.setHeader(&rightBlockHeader);

    leftBlockHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    leftBlockHeader.rblock = rightBlockNum;
    leftBlock.setHeader(&leftBlockHeader);

    for (int i = 0; i <= MIDDLE_INDEX_LEAF; i++)
        leftBlock.setEntry(&indices[i], i);

    for (int i = MIDDLE_INDEX_LEAF+1; i < MAX_KEYS_LEAF; i++)
        rightBlock.setEntry(&indices[i], i-MIDDLE_INDEX_LEAF-1);

    return rightBlockNum;  
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    int attrType = attrCatBuf.attrType;

    IndInternal intBlock(intBlockNum);

    HeadInfo blockHeader;
    intBlock.getHeader(&blockHeader);

    int numEntries = blockHeader.numEntries;

    InternalEntry internalEntries[numEntries + 1];

    int index = 0;
    int iter = 0;
    int newEntryLocation = -1;

    for (iter = 0; iter < numEntries; iter++) {
        InternalEntry currentEntry;
        intBlock.getEntry(&currentEntry, iter);

        int cmpVal = compareAttrs(currentEntry.attrVal, intEntry.attrVal, attrType);
        if (cmpVal <= 0)
            internalEntries[index++] = currentEntry;
        else
            break;
    }

    newEntryLocation = index;
    internalEntries[index++] = intEntry;

    for (; iter < numEntries; iter++) {
        InternalEntry currentEntry;
        intBlock.getEntry(&currentEntry, iter);

        if (index == newEntryLocation+1)
            currentEntry.lChild = intEntry.rChild;
        internalEntries[index++] = currentEntry;
    }


    if (numEntries != MAX_KEYS_INTERNAL) {
        blockHeader.numEntries++;
        intBlock.setHeader(&blockHeader);

        for (int i = 0; i <= numEntries; i++)
            intBlock.setEntry(&internalEntries[i], i);

        return SUCCESS;
    }
    
    int newRightBlock = splitInternal(intBlockNum, internalEntries);
    if (newRightBlock == E_DISKFULL) {
        BPlusTree::bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1)
        return BPlusTree::insertIntoInternal(relId, attrName, blockHeader.pblock, intEntry);
    else
        return createNewRoot(relId, attrName, intEntry.attrVal, intBlockNum, newRightBlock);

    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    IndInternal rightBlock;
    IndInternal leftBlock(intBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = intBlockNum;

    if (rightBlockNum == E_DISKFULL)
        return E_DISKFULL;

    HeadInfo leftBlockHeader, rightBlockHeader;
    leftBlock.getHeader(&leftBlockHeader);
    rightBlock.getHeader(&rightBlockHeader);

    rightBlockHeader.numEntries = (MAX_KEYS_INTERNAL/2);
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlock.setHeader(&rightBlockHeader);

    leftBlockHeader.numEntries = (MAX_KEYS_INTERNAL/2);
    leftBlock.setHeader(&leftBlockHeader);

    for (int i = 0; i < MIDDLE_INDEX_INTERNAL; i++)
        leftBlock.setEntry(&internalEntries[i], i);

    for (int i = MIDDLE_INDEX_INTERNAL+1; i < MAX_KEYS_INTERNAL; i++) {
        rightBlock.setEntry(&internalEntries[i-MIDDLE_INDEX_INTERNAL], i);

        int rightChildBlockNum = internalEntries[i-MIDDLE_INDEX_INTERNAL].rChild;
        BlockBuffer rightChildBlock(rightChildBlockNum);

        HeadInfo childBlockHeader;
        rightChildBlock.getHeader(&childBlockHeader);
        childBlockHeader.pblock = rightBlockNum;
        rightChildBlock.setHeader(&childBlockHeader);
    }

    return rightBlockNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    IndInternal newRootBlock;
    int newRootBlockNum = newRootBlock.getBlockNum();

    if (newRootBlockNum == E_DISKFULL) {
        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    HeadInfo newBlockHeader;
    newRootBlock.getHeader(&newBlockHeader);
    newBlockHeader.numEntries = 1;
    newRootBlock.setHeader(&newBlockHeader);

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