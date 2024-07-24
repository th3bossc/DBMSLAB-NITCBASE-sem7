#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"

void printRelAttr() {
  for (int i = 0; i < 2; i++) {
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(i, &relCatBuf);

    printf("Relation: %s\n", relCatBuf.relName);

    for (int j = 0; j < relCatBuf.numAttrs; j++) {
      AttrCatEntry attrCatBuf;
      AttrCacheTable::getAttrCatEntry(i, j, &attrCatBuf);
      const char* attrType = (attrCatBuf.attrType == NUMBER)
        ? "NUM"
        : "STR";
      printf("\t%s : %s\n", attrCatBuf.attrName, attrType);
    }
  }
}

int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  printRelAttr();
  // exercise2();
  // printAllRelationsAndAttributes();

  return FrontendInterface::handleFrontend(argc, argv);
}