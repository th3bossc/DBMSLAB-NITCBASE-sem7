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