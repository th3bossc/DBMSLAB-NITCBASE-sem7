#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>

void copyArray(int* dest, unsigned char* src, int len) {
  for (int i = 0; i < len; i++)
    dest[i] = (int)src[i];
}


int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  // StaticBuffer buffer;
  // OpenRelTable cache;

  int blockAllocationMap[4*BLOCK_SIZE];

  for (int i = 0; i < 4; i++) {
    unsigned char buffer[BLOCK_SIZE];
    
    Disk::readBlock(buffer, i);

    copyArray(blockAllocationMap+i*BLOCK_SIZE, buffer, BLOCK_SIZE);

  }
  for (int i = 0; i < 100; i++) {
    std::cout << i << " ";
    std::cout << ((blockAllocationMap[i] == 4) 
      ? "BMAP"
      : "UNUSED BLOCK")
    << std::endl;
  }
  return FrontendInterface::handleFrontend(argc, argv);
}