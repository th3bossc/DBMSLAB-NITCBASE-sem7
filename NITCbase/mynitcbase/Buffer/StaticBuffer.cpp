#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        metainfo[i].free = true;
    }
}

StaticBuffer::~StaticBuffer() {}

int StaticBuffer::getFreeBuffer(int blockNum) {
    if (blockNum < 0 || blockNum > DISK_BLOCKS)
        return E_OUTOFBOUND;

    int allocatedBuffer = -1;

    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].free) {
            allocatedBuffer = i;
            break;
        }
    }

    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].blockNum = blockNum;

    return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if (blockNum < 0 || blockNum > DISK_BLOCKS)
        return E_OUTOFBOUND;
    
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].blockNum == blockNum)
            return i;
    }

    return E_BLOCKNOTINBUFFER;
}