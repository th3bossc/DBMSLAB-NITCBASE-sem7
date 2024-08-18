#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <iostream>

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    int diff;
    (attrType == NUMBER)
        ? diff = attr1.nVal - attr2.nVal
        : diff = strcmp(attr1.sVal, attr2.sVal);
    if (diff > 0)
        return 1; // attr1 > attr2
    else if (diff < 0)
        return -1; //attr 1 < attr2
    else 
        return 0;
}



BlockBuffer::BlockBuffer(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        this->blockNum = E_DISKFULL;
    else 
        this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType) {
    int blockTypeInt;

    switch(blockType) {
        case 'R':
            blockTypeInt = REC;
            break;
        case 'I':
            blockTypeInt = IND_INTERNAL;
            break;
        case 'L':
            blockTypeInt = IND_LEAF;
            break;
        default:
            blockTypeInt  = UNUSED_BLK;
            break;
    }

    int blockNum = getFreeBlock(blockTypeInt);
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        printf("Error: Block is not available\n");
        this->blockNum = blockNum;
        return;
    }

    this->blockNum = blockNum;
}


RecBuffer::RecBuffer(int blockNum) : BlockBuffer(blockNum) {}

RecBuffer::RecBuffer() : BlockBuffer('R') {}

IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}

IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}


int BlockBuffer::getBlockNum() {
    return this->blockNum;
}

int BlockBuffer::getHeader(HeadInfo* head) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    memcpy(&head->pblock, bufferPtr + 4, 4);
	memcpy(&head->lblock, bufferPtr + 8, 4);
	memcpy(&head->rblock, bufferPtr + 12, 4);
	memcpy(&head->numEntries, bufferPtr + 16, 4);
	memcpy(&head->numAttrs, bufferPtr + 20, 4);
	memcpy(&head->numSlots, bufferPtr + 24, 4);

	return SUCCESS;
}

int BlockBuffer::setHeader(HeadInfo* head) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    struct HeadInfo* bufferHeader = (struct HeadInfo*) bufferPtr;
	bufferHeader->blockType = head->blockType;
	bufferHeader->lblock = head->lblock;
	bufferHeader->rblock = head->rblock;
	bufferHeader->pblock = head->pblock;
	bufferHeader->numAttrs = head->numAttrs;
	bufferHeader->numEntries = head->numEntries;
	bufferHeader->numSlots = head->numSlots;

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int RecBuffer::getRecord(union Attribute* record, int slotNum) {
    HeadInfo head;
    BlockBuffer::getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    int recordSize = attrCount*ATTR_SIZE;
    unsigned char* recordPtr = bufferPtr + HEADER_SIZE + slotCount + (recordSize*slotNum);
    memcpy(record, recordPtr, recordSize);

    return SUCCESS;
} 

int RecBuffer::setRecord(union Attribute* record, int slotNum) {
        HeadInfo head;
    BlockBuffer::getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    int recordSize = attrCount*ATTR_SIZE;
    unsigned char* recordPtr = bufferPtr + HEADER_SIZE + slotCount + (recordSize*slotNum);
    memcpy(recordPtr, record, recordSize);

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr) {
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
    if (bufferNum == E_OUTOFBOUND)
        return E_OUTOFBOUND;

    if (bufferNum != E_BLOCKNOTINBUFFER) {
        for (int i = 0; i < BUFFER_CAPACITY; i++)
            StaticBuffer::metainfo[i].timeStamp++;

        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }
    else {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
        if (bufferNum == E_OUTOFBOUND || bufferNum == FAILURE)
            return bufferNum;
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }

    *bufferPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char* slotMap) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) 
        return ret;

    HeadInfo head;
    getHeader(&head);

    int slotCount = head.numSlots;
    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMap, slotMapInBuffer, slotCount);
    return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char* slotMap) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    HeadInfo head;
    getHeader(&head);

    int slotCount = head.numSlots;
    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMapInBuffer, slotMap, slotCount);
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::setBlockType(int blockType) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    (* (int32_t*)bufferPtr) = blockType;

    StaticBuffer::blockAllocMap[this->blockNum] = blockType;
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getFreeBlock(int blockType) {
    int blockNum = -1;
    for (int i = 0; i < DISK_BLOCKS; i++) {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
            blockNum = i;
            break;
        }
    }

    if (blockNum == -1)
        return E_DISKFULL;

    this->blockNum = blockNum;

    int bufferIndex = StaticBuffer::getFreeBuffer(blockNum);
    if (bufferIndex < 0 || bufferIndex >= BUFFER_CAPACITY) {
        printf("Error: Buffer is full\n");
        return bufferIndex;
    }

    HeadInfo head;
    head.lblock = -1;
    head.pblock = -1;
    head.rblock = -1;
    head.numAttrs = 0;
    head.numEntries = 0;
    head.numSlots = 0;
    setHeader(&head);

    setBlockType(blockType);
    return blockNum;
}

void BlockBuffer::releaseBlock() {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS || StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
        return;

    int bufferIndex = StaticBuffer::getBufferNum(blockNum);
    if (bufferIndex >= 0 && bufferIndex < BUFFER_CAPACITY)
        StaticBuffer::metainfo[bufferIndex].free = true;

    StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;
    this->blockNum = INVALID_BLOCKNUM;
}

int IndInternal::getEntry(void* ptr, int indexNum) {
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    struct InternalEntry* internalEntry = (struct InternalEntry*) ptr;
    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*20);

    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr+4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr+20, sizeof(int32_t));

    return SUCCESS;
}

int IndInternal::setEntry(void* ptr, int indexNum) {
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    struct InternalEntry* internalEntry = (struct InternalEntry*) ptr;
    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*20);

    memcpy(entryPtr, &(internalEntry->lChild), sizeof(int32_t));
    memcpy(entryPtr+4, &(internalEntry->attrVal), sizeof(Attribute));
    memcpy(entryPtr+20, &(internalEntry->rChild), sizeof(int32_t));

    return SUCCESS;
}

int IndLeaf::getEntry(void* ptr, int indexNum) {
    if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    struct Index* index = (struct Index*) ptr;
    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*LEAF_ENTRY_SIZE);

    memcpy(index, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}

int IndLeaf::setEntry(void* ptr, int indexNum) {
        if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    struct Index* index = (struct Index*) ptr;
    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*LEAF_ENTRY_SIZE);

    memcpy(entryPtr, index, LEAF_ENTRY_SIZE);
    
    return SUCCESS;
}