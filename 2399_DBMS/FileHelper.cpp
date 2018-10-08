#include <iostream>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <sstream>
#include <fstream>
#include "dbtproj.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "FileHelper.h"
using namespace std;

/**
 * Function that is used for comparison, return 0 if the field of a and b are equal, -1 if the field of b > a
 * and 1 if the field of a > b.
 */
int compare (unsigned char FIELD, const void * a,const void * b)
{
  if (FIELD == 1) //compare for num
  {

	  if( ((record_t*)a)->num > ((record_t*)b)->num ) return 1;
	  else if ( ((record_t*)a)->num < ((record_t*)b)->num ) return -1;
	  else return 0;

  }
  else if (FIELD == 0) //compare for recid
    {
  	  if( ((record_t*)a)->recid > ((record_t*)b)->recid ) return 1;
  	  else if ( ((record_t*)a)->recid < ((record_t*)b)->recid ) return -1;
  	  else return 0;

    }
  else if (FIELD == 2) //compare for str
      {
	  	  return strcmp(((record_t*)a)->str ,((record_t*)b)->str);
      }
  else //compare for both num and str
  {
	  if( ((record_t*)a)->num > ((record_t*)b)->num ) return 1; //first compare for num
	  	  	  else if ( ((record_t*)a)->num < ((record_t*)b)->num ) return -1;
	  	  	  else  //compare for str
	  	  	  {
	  	  		return strcmp(((record_t*)a)->str ,((record_t*)b)->str);
	  	  	  }
  }
}

/**
 * Function that swaps the records buffer[blockID].entries[i] and buffer[storeBlock].entries[storeID].
 * @param buffer
 * @param i
 * @param storeID
 * @param blockID
 * @param storeBlock
 */
 void swap(block_t *buffer, int i, int storeID, int blockID, int storeBlock)
 {
     record_t temp;
     temp = buffer[blockID].entries[i];
     buffer[blockID].entries[i] = buffer[storeBlock].entries[storeID];
     buffer[storeBlock].entries[storeID] = temp;
 }

 /**
  * Quicksort for buffer.
  *This function finds the position of pivot in the buffer from startOfBlock startIndex,
  * to endOfBlock endIndex. endIndex record of endOfBlock is set as pivot.
  * @param buffer
  * @param startOfBlock
  * @param startIndex
  * @param endOfBlock
  * @param endIndex
  */
 void QuicksortBuffer(unsigned char field,block_t *buffer,int startOfBlock, int startIndex, int endOfBlock, int endIndex){
     if (startOfBlock >= endOfBlock && startIndex >= endIndex)
     {
         return;
     }
     else
     {
         int blockId;   //id of the block that we start the sorting
         int store = startIndex;    //keep tracks of the position the pivot is in every loop
         int storeBlock = startOfBlock;  //the block that the store pointer belongs to
         int pivot = endIndex;  //choose the last element of the last block as pivot
         int i = startIndex;

         //It finds the right position of the element pivot, in the blocks of the buffer.
         //Any element before the the pivot is lesser or equal than it and any element after pivot is greater than it.
         for (blockId = startOfBlock; blockId <= endOfBlock; blockId++) {
            //for every block from start_block to end_block in buffer
            while ( i < buffer[blockId].nreserved ) {
                if(blockId == endOfBlock && i == endIndex)
                {//if we reached the end of blocks and indexes
                    break;
                }
                else
                {
                //for every element in the current block
                if (compare(field,&(buffer[blockId].entries[i]), &(buffer[endOfBlock].entries[endIndex])) <= 0)
                {
                    //if it is lesser of or equal to pivot
                    //swap the element at i with the element at store
                    swap(buffer, i, store, blockId, storeBlock);

                    if(store+1 == buffer[storeBlock].nreserved && storeBlock+1 <= endOfBlock)
                    {//if store has reached the end of the block and there is another block after that
                     //make store point to the start of the next block
                        store = 0;
                        storeBlock++;
                    }
                    else if (store+1 < buffer[storeBlock].nreserved)
                    {
                     //if store has not reached the end of the block yet,increase it
                        store++;
                    }
                }
                }
                i++;
            }
            i = 0;
        }
         //storeBlock shows the right block and store the right index in the block for pivot
         swap(buffer, endIndex, store, endOfBlock, storeBlock);

         //call QuicksortBuffer for the indexes before and after the pivot
         if(store == 0 && storeBlock > startOfBlock)
         {//if store is the first index of the block and there are more blocks before this one
             QuicksortBuffer(field,buffer, startOfBlock, startIndex, storeBlock-1, buffer[storeBlock-1].nreserved-1);
         }
         else
         {//call QuicksortBuffer for the blocks from start to pivot
             QuicksortBuffer(field,buffer, startOfBlock, startIndex, storeBlock, store-1);
         }
         if(store == buffer[storeBlock].nreserved-1 && storeBlock < endOfBlock)
         {//if pivot is the last of the block and there are more blocks after it
             QuicksortBuffer(field,buffer, storeBlock+1, 0 , endOfBlock, endIndex);
         }
         else
         {//call QuicksortBuffer for the blocks from pivot to end
             QuicksortBuffer(field,buffer, storeBlock, store+1, endOfBlock, endIndex);
         }
     }

 }
