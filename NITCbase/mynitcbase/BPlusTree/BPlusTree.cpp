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

    if (attrCatBuf.rootBlock != -1)
        return SUCCESS;

    IndLeaf rootBlockBuf;

    int rootBlock = rootBlockBuf.getBlockNum();
    if (rootBlock == E_DISKFULL)
        return E_DISKFULL;

    attrCatBuf.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    RelCatEntry relCatEntry;
    ret = RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    if (ret != SUCCESS)
        return ret;

    int block = relCatEntry.firstBlk;

    while (block != -1) {
        RecBuffer currentBlock(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        currentBlock.getSlotMap(slotMap);

        for (int i = 0; i < relCatEntry.numSlotsPerBlk; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED)
                continue;

            Attribute record[relCatEntry.numAttrs];
            currentBlock.getRecord(record, i);


            RecId recId = {block, i};
            int ret = BPlusTree::bPlusInsert(relId, attrName, record[attrCatBuf.offset], recId);
            if (ret == E_DISKFULL)
                return E_DISKFULL;
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
        IndLeaf rootNode(rootBlockNum);

        rootNode.releaseBlock();
        return SUCCESS;
    }
    else if (type == IND_INTERNAL) {
        IndInternal rootNode(rootBlockNum);

        HeadInfo rootHeader;
        rootNode.getHeader(&rootHeader);

        InternalEntry indEntry;
        rootNode.getEntry(&indEntry, 0);

        if (indEntry.lChild != -1) {
            int ret = bPlusDestroy(indEntry.lChild);
            if (ret != SUCCESS)
                return ret;
        }
            

        int numEntries = rootHeader.numEntries;
        for (int i = 0; i < numEntries; i++) {
            rootNode.getEntry(&indEntry, i);
            if (indEntry.rChild != -1) {
                int ret = bPlusDestroy(indEntry.rChild);
                if (ret != SUCCESS)
                    return ret;
            }
        }

        return SUCCESS;
    }
    else {
        return E_INVALIDBLOCK;
    }
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

    Index entry;
    entry.attrVal = attrVal;
    entry.block = recId.block;
    entry.slot = recId.slot;



    ret = insertIntoLeaf(relId, attrName, leafBlockNum, entry);
    if (ret == E_DISKFULL) {
        BPlusTree::bPlusDestroy(blockNum);
        attrCatBuf.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);
        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;
    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {

        IndInternal intBlock(blockNum);

        HeadInfo intHeader;
        intBlock.getHeader(&intHeader);

        int numEntries = intHeader.numEntries;
        InternalEntry intEntry;

        int targetIndex = -1;
        for (int i = 0; i < numEntries; i++) {
            intBlock.getEntry(&intEntry, i);

            if (compareAttrs(intEntry.attrVal, attrVal, attrType) > 0) {
                targetIndex = i;
                break;
            }
        }

        if (targetIndex == -1) {
            intBlock.getEntry(&intEntry, numEntries-1);
            blockNum = intEntry.rChild;
        }
        else {
            intBlock.getEntry(&intEntry, targetIndex);
            blockNum = intEntry.lChild;
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

    Index indices[numEntries + 1];

    int targetIndex = numEntries;
    Index leafEntry;
    for (int i = 0; i < numEntries; i++) {
        leafBlock.getEntry(&leafEntry, i);
        if (compareAttrs(leafEntry.attrVal, indexEntry.attrVal, attrCatBuf.attrType) > 0) {
            targetIndex = i;
            break;
        }
    }

    for (int i = 0; i < targetIndex; i++)
        leafBlock.getEntry(&indices[i], i);

    indices[targetIndex] = indexEntry;

    for (int i = targetIndex; i < numEntries; i++)
        leafBlock.getEntry(&indices[i+1], i);

    if (numEntries != MAX_KEYS_LEAF) {
        leafHeader.numEntries++;
        leafBlock.setHeader(&leafHeader);

        for (int i = 0; i < leafHeader.numEntries; i++)
            leafBlock.setEntry(&indices[i], i);

        return SUCCESS;
    }

    int newRightBlock = splitLeaf(blockNum, indices);
    if (newRightBlock == E_DISKFULL)
        return newRightBlock;

    if (leafHeader.pblock != -1) {
        InternalEntry intEntry;
        intEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        intEntry.lChild = blockNum;
        intEntry.rChild = newRightBlock;

        return insertIntoInternal(relId, attrName, leafHeader.pblock, intEntry);
    }
    else {
        return createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlock);
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    IndLeaf rightBlock;
    IndLeaf leftBlock(leafBlockNum);

    int leftBlockNum = leafBlockNum;
    int rightBlockNum = rightBlock.getBlockNum();

    if (rightBlockNum == E_DISKFULL)
        return E_DISKFULL;

    HeadInfo leftBlockHeader, rightBlockHeader;
    rightBlock.getHeader(&rightBlockHeader);
    leftBlock.getHeader(&leftBlockHeader);

    rightBlockHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlockHeader.lblock = leftBlockNum;
    rightBlockHeader.rblock = leftBlockHeader.rblock;
    rightBlock.setHeader(&rightBlockHeader);

    leftBlockHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    leftBlockHeader.rblock = rightBlockNum;
    leftBlock.setHeader(&leftBlockHeader);

    for (int i = 0; i < 32; i++)    
        leftBlock.setEntry(&indices[i], i);

    for (int i = 32; i < 64; i++)
        rightBlock.setEntry(&indices[i], i-32);

    return rightBlockNum; 
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {

    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    if (ret != SUCCESS)
        return ret;

    IndInternal intBlock(intBlockNum);
    
    HeadInfo intHeader;
    intBlock.getHeader(&intHeader);


    InternalEntry intEntries[intHeader.numEntries + 1];

    int targetIndex = intHeader.numEntries;
    InternalEntry entryBuffer;
    for (int i = 0; i < intHeader.numEntries; i++) {
        intBlock.getEntry(&entryBuffer, i);
        if (compareAttrs(entryBuffer.attrVal, intEntry.attrVal, attrCatBuf.attrType) > 0) {
            targetIndex = i;
            break;
        }
    }


    for (int i = 0; i < targetIndex; i++)
        intBlock.getEntry(&intEntries[i], i);
    
    intEntries[targetIndex] = intEntry;

    for (int i = targetIndex; i < intHeader.numEntries; i++)
        intBlock.getEntry(&intEntries[i+1], i);

    if (targetIndex < intHeader.numEntries)
        intEntries[targetIndex+1].lChild = intEntries[targetIndex].rChild;

    if (intHeader.numEntries != MAX_KEYS_INTERNAL) {
        intHeader.numEntries++;
        intBlock.setHeader(&intHeader);

        for (int i = 0; i < intHeader.numEntries; i++)
            intBlock.setEntry(&intEntries[i], i);

        return SUCCESS;
    }



    int newRightBlock = splitInternal(intBlockNum, intEntries);
    if (newRightBlock == E_DISKFULL)
        return E_DISKFULL;

    

    if (intHeader.pblock != -1) {
        InternalEntry entryInParent;
        entryInParent.attrVal = intEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        entryInParent.lChild = intBlockNum;
        entryInParent.rChild = newRightBlock;

        return insertIntoInternal(relId, attrName, intHeader.pblock, entryInParent);
    }
    else {
        return createNewRoot(relId, attrName, intEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlock);
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

    for (int i = MIDDLE_INDEX_INTERNAL+1; i <= 100; i++)
        rightBlock.setEntry(&internalEntries[i], i-MIDDLE_INDEX_INTERNAL-1);

    InternalEntry entryBuffer;
    rightBlock.getEntry(&entryBuffer, 0);
    BlockBuffer childBuffer(entryBuffer.lChild);
    HeadInfo childHeader;
    childBuffer.getHeader(&childHeader);
    childHeader.pblock = rightBlockNum;
    childBuffer.setHeader(&childHeader);

    for (int i = 0; i < rightBlockHeader.numEntries; i++) {
        rightBlock.getEntry(&entryBuffer, i);
        BlockBuffer childBuffer(entryBuffer.rChild);
        HeadInfo childHeader;
        childBuffer.getHeader(&childHeader);
        childHeader.pblock = rightBlockNum;
        childBuffer.setHeader(&childHeader);
    }

    return rightBlockNum;
}


int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    IndInternal newRootBlock;
    int newRootBlockNum = newRootBlock.getBlockNum();

    if (newRootBlockNum == E_DISKFULL) {
        BPlusTree::bPlusDestroy(lChild);
        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }  

    HeadInfo newRootHeader;
    newRootBlock.getHeader(&newRootHeader);
    newRootHeader.numEntries = 1;
    newRootBlock.setHeader(&newRootHeader);

    InternalEntry intEntry;
    intEntry.attrVal = attrVal;
    intEntry.lChild = lChild;
    intEntry.rChild = rChild;

    newRootBlock.setEntry(&intEntry, 0);

    BlockBuffer leftChild(lChild), rightChild(rChild);
    HeadInfo leftHeader, rightHeader;

    leftChild.getHeader(&leftHeader);
    leftHeader.pblock = newRootBlockNum;
    leftChild.setHeader(&leftHeader);

    rightChild.getHeader(&rightHeader);
    rightHeader.pblock = newRootBlockNum;
    rightChild.setHeader(&rightHeader);

    attrCatBuf.rootBlock = newRootBlockNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuf);

    return SUCCESS;
}