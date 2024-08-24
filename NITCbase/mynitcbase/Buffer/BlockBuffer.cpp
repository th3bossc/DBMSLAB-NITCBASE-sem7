#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    double diff;
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
    this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType) {
    int blockTypeNum;
    if (blockType == 'R')
        blockTypeNum = REC;
    else if (blockType == 'I')
        blockTypeNum = IND_INTERNAL;
    else if (blockType == 'L')
        blockTypeNum = IND_LEAF;
    else 
        blockTypeNum = UNUSED_BLK;
    int blockNum = getFreeBlock(blockTypeNum);

    this->blockNum = blockNum;
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return;
}


RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R') {}

int BlockBuffer::getHeader(struct HeadInfo* head) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);    

    if (ret != SUCCESS)
        return ret;

    HeadInfo* header = (HeadInfo*) bufferPtr;

    head->numSlots = header->numSlots;
    head->numEntries = header->numEntries;
    head->numAttrs = header->numAttrs;
    head->lblock = header->lblock;
    head->rblock = header->rblock;
    head->pblock = header->pblock;

    return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo* head) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret;

    HeadInfo* header = (HeadInfo*) bufferPtr;

    header->numSlots = head->numSlots;
    header->numEntries = head->numEntries;
    header->numAttrs = head->numAttrs;
    header->lblock = head->lblock;
    header->rblock = head->rblock;
    header->pblock = head->pblock;

    return StaticBuffer::setDirtyBit(this->blockNum);
}


int RecBuffer::getRecord(union Attribute* rec, int slotNum) {
    struct HeadInfo head;

    this->getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    // header size -> 32
    // slotMapSize -> numSlots
    // index -> recordSize*slotNum
    int recordSize = attrCount * ATTR_SIZE;
    unsigned char* slotPointer = bufferPtr + 32 + slotCount + recordSize*slotNum;
    memcpy(rec, slotPointer, recordSize);
    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute* rec, int slotNum) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret;

    HeadInfo header;
    this->getHeader(&header);

    int numAttrs = header.numAttrs;
    int numSlots = header.numSlots;

    if (slotNum < 0 || slotNum >= numSlots)
        return E_OUTOFBOUND;
    
    int recordSize = numAttrs*ATTR_SIZE;
    unsigned char* recordPtr = bufferPtr + HEADER_SIZE + numSlots + slotNum*recordSize;

    memcpy(recordPtr, rec, recordSize);
    StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char* slotMap) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret;

    struct HeadInfo head;
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

    struct HeadInfo head;
    getHeader(&head);

    int slotCount = head.numSlots;

    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;

    memcpy(slotMapInBuffer, slotMap, slotCount);

    return StaticBuffer::setDirtyBit(this->blockNum);

}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr) {
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum == E_BLOCKNOTINBUFFER) {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND)
            return E_OUTOFBOUND;

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    else {
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            if (!StaticBuffer::metainfo[i].free)
                StaticBuffer::metainfo[i].timeStamp++;
        }

        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }

    *bufferPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}

int BlockBuffer::getBlockNum() {
    return this->blockNum;  
}

int BlockBuffer::setBlockType(int blockType) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret;

    *((int32_t*)bufferPtr) = blockType;

    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getFreeBlock(int blockType) {
    int freeBlock = -1;
    for (int i = 0; i < DISK_BLOCKS; i++) {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
            freeBlock = i;
            break;
        }
    }

    if (freeBlock == -1)
        return E_DISKFULL;

    this->blockNum = freeBlock;

    int freeBuffer = StaticBuffer::getFreeBuffer(freeBlock);

    HeadInfo header;
    header.pblock = -1;
    header.lblock = -1;
    header.rblock = -1;
    header.numEntries = 0;
    header.numAttrs = 0;
    header.numSlots = 0;

    this->setHeader(&header);
    this->setBlockType(blockType);


    return freeBlock;
}

void BlockBuffer::releaseBlock() {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS || StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
        return;


    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    if (bufferNum == E_BLOCKNOTINBUFFER)
        return;

    StaticBuffer::metainfo[bufferNum].free = true;

    StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;

    this->blockNum = INVALID_BLOCKNUM;
}

IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}

IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

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
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr + 4 + ATTR_SIZE, sizeof(int32_t));
    
    return SUCCESS;
}

int IndInternal::setEntry(void* ptr, int indexNum) {
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    InternalEntry* internalEntry = (InternalEntry*) ptr;
    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*20);

    memcpy(entryPtr, &(internalEntry->lChild), sizeof(int32_t));
    memcpy(entryPtr + 4, &(internalEntry->attrVal), sizeof(Attribute));
    memcpy(entryPtr + 4 + ATTR_SIZE, &(internalEntry->rChild), sizeof(int32_t));

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int IndLeaf::getEntry(void* ptr, int indexNum) {
    if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*LEAF_ENTRY_SIZE);
    memcpy((struct Index*) ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}

int IndLeaf::setEntry(void* ptr, int indexNum) {
    if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
        return E_OUTOFBOUND;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret;

    Index* index = (Index*) ptr;

    unsigned char* entryPtr = bufferPtr + HEADER_SIZE + (indexNum*LEAF_ENTRY_SIZE);
    memcpy(entryPtr, index, LEAF_ENTRY_SIZE);

    return StaticBuffer::setDirtyBit(this->blockNum);
}
