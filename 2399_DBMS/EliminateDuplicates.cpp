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

/* ----------------------------------------------------------------------------------------------------------------------
   infile: the name of the input file
   field: which field will be used for sorting: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nunique: number of unique records in file (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
*/
void EliminateDuplicates (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nunique, unsigned int *nios)
{

    unsigned int nsorted_segs=0, npasses=0;

    MergeSort(infile, field, buffer, nmem_blocks, "temp.bin", &nsorted_segs, &npasses, nios);   //call MergeSort


    record_t previousRecord;   //record keeps the tracking of the last record

    previousRecord.valid = 0;  //set previousRecord valid to 0 for the comparison with the first record

    int lastBlock_i = 0;    //a variable to keep the tracking of the last record in last block of buffer

    int id = 0; //a variable to keep the tracking of the last block's id every time we write it to the output file

    //open the sorted file for reading
    FILE *input;
    input = fopen("temp.bin", "r");

    //open the output fie for writing
    FILE *output;
    output = fopen(outfile, "w");

    while (!feof(input))
    {
        //fill buffer
        int i = 0;
        while (i < nmem_blocks - 1 && fread(&buffer[i], 1, sizeof(block_t), input))
        {
            (*nios)++;
            i++;
        }

        //for every block in buffer, parse each record keeping the tracking of previous
        //and write to the last block of buffer if previous record is different than current record
        for (int j = 0; j < i; j++)
        {
            for (int k = 0; k < buffer[j].nreserved; k++)
            {
                //The function compare returns 0 if the records have the same value in the field we specified
                if (compare(field,&(buffer[j].entries[k]), &(previousRecord)))
                {
                    (*nunique)++;
                    //records are different in that field
                    //write that record in the last block of buffer
                    buffer[nmem_blocks - 1].entries[lastBlock_i] = buffer[j].entries[k];
                    lastBlock_i++;

                    //check if last block is full
                    if (lastBlock_i == MAX_RECORDS_PER_BLOCK)
                    {
                        //write last block to output file
                        buffer[nmem_blocks - 1].blockid = id;
                        buffer[nmem_blocks - 1].nreserved = MAX_RECORDS_PER_BLOCK;
                        fwrite(&buffer[nmem_blocks - 1], 1, sizeof (block_t), output);
                        (*nios)++;
                        id++;
                        lastBlock_i = 0;
                    }
                }
                //change previous record to current record
                previousRecord = buffer[j].entries[k];
            }
        }
    }

    //if there are records in the last block not written to the output file
    if(lastBlock_i > 0)
    {
       //write the last block to the output file
       buffer[nmem_blocks - 1].blockid = id;
       buffer[nmem_blocks - 1].nreserved = lastBlock_i;
       fwrite(&buffer[nmem_blocks - 1], 1, sizeof (block_t), output);
       (*nios)++;
    }

    //close the files
    fclose(input);
    fclose(output);
    remove("temp.bin");
}
