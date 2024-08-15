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
        index = searchIndex.index + 1;

        IndLeaf leaf(block);

        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            block = leafHead.rblock;
            index = 0;
            if (block == -1) {
                return RecId({-1, -1});
            }
        }
    }

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {
        IndInternal internalBlock(block);

        HeadInfo intHead;
        internalBlock.getHeader(&intHead);

        InternalEntry intEntry;
        internalBlock.getEntry(&intEntry, 0);

        if (
            op == NE || 
            op == LT ||
            op == LE
        ) {
            block = intEntry.lChild;
        }
        else {
            int targetIndex = -1;
            for (int i = 1; i < intHead.numEntries; i++) {
                InternalEntry currentIntEntry;
                internalBlock.getEntry(&currentIntEntry, i);

                int cmpVal = compareAttrs(currentIntEntry.attrVal, attrVal, attrCatEntry.attrType);

                if (
                    (op == EQ && cmpVal == 0)   ||
                    (op == GT && cmpVal > 0)    ||
                    (op == GE && cmpVal >= 0)
                ) {
                    targetIndex = i;
                    break;
                }
            }

            if (targetIndex != -1) {
                InternalEntry targetInternalEntry;
                internalBlock.getEntry(&targetInternalEntry, targetIndex);

                block = targetInternalEntry.lChild;
            }
            else {
                InternalEntry targetInternalEntry;
                internalBlock.getEntry(&targetInternalEntry, intHead.numEntries-1);

                block = targetInternalEntry.rChild;
            }
        }
    }

    while (block != -1) {
        IndLeaf leafBlock(block);

        HeadInfo leafHead;
        leafBlock.getHeader(&leafHead);

        Index leafEntry;
        leafBlock.getEntry(&leafEntry, index);

        int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

        while (index < leafHead.numEntries) {
            if (
                (op == EQ && cmpVal == 0)   ||
                (op == LE && cmpVal <= 0)   ||
                (op == LT && cmpVal < 0)    ||
                (op == GT && cmpVal > 0)    ||
                (op == GE && cmpVal >= 0)   ||
                (op == NE && cmpVal != 0)
            ) {
                searchIndex.block = block;
                searchIndex.index = index;

                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);

                return RecId({leafEntry.block, leafEntry.slot});
            }
            else if(
                (op == EQ || op == LE || op == LT) && cmpVal > 0
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