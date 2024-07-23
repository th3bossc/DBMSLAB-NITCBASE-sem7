#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"


void printAllRelationsAndAttributes() {
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  HeadInfo relCatHeader;

  relCatBuffer.getHeader(&relCatHeader);

  for (int i = 0; i < relCatHeader.numEntries; i++) {
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(relCatRecord, i);


    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    int attrCatBlockNumber = ATTRCAT_BLOCK;
    while (attrCatBlockNumber != -1) {
      RecBuffer attrCatBuffer(attrCatBlockNumber);
      HeadInfo attrCatHeader;
      attrCatBuffer.getHeader(&attrCatHeader);
      attrCatBlockNumber = attrCatHeader.rblock;


      for (int j = 0; j < attrCatHeader.numEntries; j++) {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord, j);
        if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal))
          continue;
          

        const char* attrType = (attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER)
          ? "NUM"
          : "STR";

        printf("%s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
    } 
    printf("\n");
  }
}

int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  printAllRelationsAndAttributes();

  return FrontendInterface::handleFrontend(argc, argv);
}