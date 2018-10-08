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
 * This function merges the sorted files from startFile till memBlocks-1, to one sorted file
 * @param buffer
 * @param startFile
 * @param memBlocks
 * @param outputFile
 */
void MergeFiles(unsigned char field,block_t *buffer,int startFile, int memBlocks, char *outputFile, unsigned int *ios)
 {
     char filename[100];
     int id = 0;

     //open file for writing the output
     FILE *output = fopen(outputFile, "w");

     //variables to keep tracking of the next record in every block
     int blockPointers[memBlocks-1];

     //set the variables to the first record of every block
     for(int i=0; i<memBlocks-1; i++)
     {
         blockPointers[i] = 0;
     }

     //array of the files
     FILE *files[memBlocks-1];

     //assign each variable of the array to a file
     for(int i=0; i<memBlocks-1; i++)
     {
         //filenames are file1,file2,file3 etc...
         sprintf(filename, "file%d", i+startFile);
         files[i] = fopen(filename, "r");
         //read the first block of the file to buffer[i]
         fread(&buffer[i], 1, sizeof(block_t), files[i]); // read the next block
         (*ios)++;
     }

     //Initialize variables for merge:
     //The variables "min_i" and "minBlock" point to the minimum record.
     int min_i=0,minBlock=0;

     //The variable "lastBlock_i" points to the next available record in the last block of.
     int lastBlock_i = 0;

     //The variable "finished" keep how many files have finished.
     int finished = 0;

     //Merge the files:
     while(finished < memBlocks-1)  //while there are unfinished files
     {
         minBlock = 0;
         min_i = blockPointers[0];
         for (int i = 0; i < memBlocks - 1; i++)    //check the first element of every block to find the minimum record
         {
            if (blockPointers[i] < buffer[i].nreserved) //if the block[i] has not reached the end of the file[i]
            {
                if (compare(field,&(buffer[minBlock].entries[min_i]), &(buffer[i].entries[blockPointers[i]])) > 0)
                {
                    //if the current min element is greater than the one compared to
                    minBlock = i;
                    min_i = blockPointers[i];
                }
            }
        }

        //write min record to the last block of the buffer
        buffer[memBlocks - 1].entries[lastBlock_i] = buffer[minBlock].entries[min_i];
        lastBlock_i++;
        blockPointers[minBlock]++;

        //check if last block is full
        if (lastBlock_i == MAX_RECORDS_PER_BLOCK)
        {
            //write the last block to the output file
            buffer[memBlocks - 1].nreserved = lastBlock_i;
            buffer[memBlocks - 1].blockid = id;
            id++;
            fwrite(&buffer[memBlocks - 1], 1, sizeof (block_t), output);
            lastBlock_i = 0;
            (*ios)++;
        }

        //check if the blockPointers of the min_i block reached the end of the block
        if (blockPointers[minBlock] == buffer[minBlock].nreserved)
        {//check if there is another block in the file of this block
            if (fread(&buffer[minBlock], 1, sizeof (block_t), files[minBlock]))
            {
                blockPointers[minBlock] = 0;
            }
            else
            {
                finished++;
                if(finished == memBlocks - 2)
                {//if there is only one not finished file
                    if (lastBlock_i > 0)
                    {
                        //write the last block to the output file
                        buffer[memBlocks - 1].nreserved = lastBlock_i;
                        buffer[memBlocks - 1].blockid = id;
                        id++;
                        fwrite(&buffer[memBlocks - 1], 1, sizeof (block_t), output);
                        lastBlock_i = 0;
                        (*ios)++;
                    }

                    //find the not finished block in the buffer
                    for(int j=0; j<memBlocks - 1; j++)
                    {
                        if (blockPointers[j] < buffer[j].nreserved)
                        {//buffer[j] has not finished yet
                            //add every record of buffer[j] to the last block and when they are finished write it to buffer[memBlocks-1].entries[lastBlock_i]
                            while(blockPointers[j] < buffer[j].nreserved)
                            {
                                buffer[memBlocks-1].entries[lastBlock_i] = buffer[j].entries[blockPointers[j]];
                                lastBlock_i++;
                                blockPointers[j]++;
                            }

                            //write last block
                            buffer[memBlocks - 1].nreserved = lastBlock_i;
                            buffer[memBlocks - 1].blockid = id;
                            fwrite(&buffer[memBlocks - 1], 1, sizeof (block_t), output);
                            lastBlock_i = 0;
                            (*ios)++;

                            //write every block from the file to the output file
                            while (fread(&buffer[0], 1, sizeof (block_t), files[j]))
                            {
                                fwrite(&buffer[0], 1, sizeof(block_t), output);
                                (*ios) += 2;
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }
     }

     //close all files and delete the input files, which have been used in the merge
     fclose(output);
     for(int i=0; i<memBlocks-1; i++)
     {
         fclose(files[i]);
         sprintf(filename, "file%d", i+startFile);
         remove(filename);
     }
 }

 /**
  * This function returns the minimum of a and b
  */
 int min(int a, int b)
 {
     if (a <= b)
         return a;
     return b;
 }


/* ----------------------------------------------------------------------------------------------------------------------
   infile: the name of the input file
   field: which field will be used for sorting: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nsorted_segs: number of sorted segments produced (this should be set by you)
   npasses: number of passes required for sorting (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
*/
void MergeSort (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nsorted_segs, unsigned int *npasses, unsigned int *nios)
{

    FILE *input,*output,*temp;
    char filename[100];

    //number of blocks contained in the input file
    int blocks = 0; //used to find the npasses

    //open the infile for reading the input
    input = fopen(infile,"r");

    if(input != NULL) //check if the file is opened and if it actually exists
    {
        //number of files that we will create for every merge
        int numOfFiles = 0;

    	while (!feof(input))
        {
            //fill buffer from input file
            int i = 0;
            while (i < nmem_blocks && fread(&buffer[i], 1, sizeof (block_t), input))
            {
                //increase i and nios
                i++;
                (*nios)++;
            }
            if (i > 0)
            {
                //call QuicksortBuffer to sort the buffer
                QuicksortBuffer(field,buffer, 0, 0, i - 1, buffer[i - 1].nreserved - 1);

                //increase nsorted_segments
                (*nsorted_segs)++;

                //add i to blocks
                blocks += i;

                //buffer is a sorted segment. Write it to a temp file
                numOfFiles++;
                sprintf(filename, "file%d", numOfFiles);
                temp = fopen(filename, "w");    //it opens the temporary file temp for writing

                //write the sorted blocks
                for (int j = 0; j < i; j++) {
                    (*nios)++;
                    fwrite(&buffer[j], 1, sizeof (block_t), temp); // write the block to the file
                }
                fclose(temp);
            }

        }
    	fclose(input);

        //calculate npasses
        blocks = blocks / nmem_blocks;
        (*npasses)++;
        while(blocks > 0)
        {
            blocks = blocks / (nmem_blocks-1);
            (*npasses)++;
        }

        if (numOfFiles == 1)    //if there was only one file created,no need for merging.
        {
            //Rename it to the filename specified in outfile.
            rename(filename, outfile);
        } else {

            //We have sorted the buffer and stored it to numOfFiles different files.
            //We have to merge them until we have only one file.
            int start = 1;
            int lastFile = numOfFiles;
            int end;

            while (numOfFiles > 1) {
                //set end to minimum of size of the buffer or number of files+1
                end = min(nmem_blocks, numOfFiles + 1);
                //if all files can be merged set the filename of the output to outfile
                if (end == numOfFiles + 1)
                    strcpy(filename, outfile);
                else
                    sprintf(filename, "file%d", lastFile + 1);

                //Merge the files.
                MergeFiles(field,buffer, start, end, filename, nios);
                //Change the variables according to how many files were merged and created.
                lastFile++;
                numOfFiles -= end - 2;
                start += end - 1;
            }
        }
    }
    else
    {//The opening of the file failed.
    	cout<<"Could not open the input file.\n";
    }
}
