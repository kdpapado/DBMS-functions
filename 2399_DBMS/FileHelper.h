#ifndef FILEHELPER_H_INCLUDED
#define FILEHELPER_H_INCLUDED
int compare (unsigned char FIELD, const void * a,const void * b);
void swap(block_t *buffer, int i, int storeID, int blockID, int storeBlock);
void QuicksortBuffer(unsigned char field,block_t *buffer,int startOfBlock, int startIndex, int endOfBlock, int endIndex);
#endif // FILEHELPER_H_INCLUDED
