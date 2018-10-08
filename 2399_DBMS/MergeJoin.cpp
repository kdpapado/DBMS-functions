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
using namespace std;


/* ----------------------------------------------------------------------------------------------------------------------
   infile1: the name of the first input file
   infile2: the name of the second input file
   field: which field will be used for the join: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nres: number of pairs in output (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
*/
void MergeJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios)
{
    unsigned int nsorted_segs = 0;
    unsigned int npasses = 0;
    char tmpFile1[] = ".mj1";
    char tmpFile2[] = ".mj2";

    /* Each one of the infiles is sorted using MergeSort.
     The files produced (".mj1" and ".mj2") are 100% utilised (with the possible
     exception of the last block of each). As a result,there is no need to take measures for invalid blocks.*/


    //First we sort the input.
    MergeSort(infile1, field, buffer, nmem_blocks, tmpFile1, &nsorted_segs, &npasses, nios);
    MergeSort(infile2, field, buffer, nmem_blocks, tmpFile2, &nsorted_segs, &npasses, nios);

    //Initialize the buffer values.
    for(int i=0;i<nmem_blocks;i++)
    {
        for(int j=0;j<MAX_RECORDS_PER_BLOCK;j++)
        {
            buffer[i].entries[j].num=0;
            buffer[i].entries[j].recid=0;
            buffer[i].entries[j].valid=false;
            strcpy(buffer[i].entries[j].str,"\0");
        }
        buffer[i].nreserved=0;
    }

    //Open the file streams:
    ifstream input_file1;
    input_file1.open(tmpFile1, ios::binary);
    ifstream input_file2;
    input_file2.open(tmpFile2, ios::binary);
    ofstream output_file;
    output_file.open(outfile, ios::binary);


    unsigned int output_buffer_size = 1; //output buffer's size in blocks
    unsigned int first_buffer_size = (nmem_blocks - output_buffer_size)/2; //the buffer for the first input in blocks
    unsigned int second_buffer_size = nmem_blocks - output_buffer_size - first_buffer_size; //the buffer for the
                                                                                            //second input in blocks

    //Initialize the cοunters:
    unsigned int firstBufBlockCtr = 0;
    unsigned int firstBufRecordCtr = 0;
    unsigned int secondBufBlockCtr=first_buffer_size;
    unsigned int secondBufRecordCtr=0;
    unsigned int outputBufBlockCtr = first_buffer_size+second_buffer_size;
    unsigned int outputBufRecordCtr = 0;

    //Initialize the pointers:
    record_t *firstBufPtr = &(buffer[firstBufBlockCtr].entries[firstBufRecordCtr]);
    record_t *secondBufPtr = &(buffer[secondBufBlockCtr].entries[secondBufRecordCtr]);
    record_t *outputBufPtr = &(buffer[outputBufBlockCtr].entries[outputBufRecordCtr]);

    int input1Done = 0; //the condition for input1
    int input2Done = 0; //the condition for input2
    bool oneInputDone=false;

    bool mustToRead1 = true; //It is true when there is the need to
                            //read more records from the first input.
    bool mustToRead2 = true; //It is true when there is the need to
                            //read more records from the second input.
    bool mustToWrite = false;//It is true when the output buffer is full and
                            //the records must get written to the output file.

    unsigned int read_records1 = 0; //It counts the already read records,
                                    //which were read by the first input.
    unsigned int read_records2 = 0;  //It counts the already read records,
                                    //which were read by the second input.
    unsigned int used_blocks1 = 0;  //It counts how many blocks are being used
                                    //after reading from the first input.
    unsigned int used_blocks2 = 0;  //It counts how many blocks are being used
                                    //after reading from the second input.
    unsigned int writtenInOutBuf=0; //It counts how many records
                                    //have been written in the output buffer.


    for(int i=first_buffer_size+second_buffer_size;i<nmem_blocks;i++)
    {
        buffer[i].valid=false;
    }
    for(int i=0;i<first_buffer_size+second_buffer_size;i++)
    {
        buffer[i].valid=true;
    }

    unsigned int ctr=0; //defines the id of each block written to output

    while (!oneInputDone) { //while none of the inputs has finished

        if(mustToRead1) //read input1 if need be
        {
            read_records1 = 0;
            int i = 0;  //the first position of the input buffer
            int sum = 0;
            while (!input_file1.eof() && i < first_buffer_size) {
                input_file1.read(reinterpret_cast<char*> (&buffer[i]), sizeof (block_t));
                sum += buffer[i].nreserved;
                if (input_file1.eof()){
                    if(i==0)
                    {
                        input1Done=1;   //if the input had finished earlier but it was bot recognized by eof()
                    }
                    else
                    {
                        input1Done=2;   //if the input has finished now
                    }
                }
                i++;
            }
            read_records1 = sum;
            (*nios)++;
            used_blocks1 = (read_records1 / MAX_RECORDS_PER_BLOCK);
            if (read_records1 % MAX_RECORDS_PER_BLOCK != 0) used_blocks1++;
            mustToRead1 = false;
        }
        if(mustToRead2) //read input2 if need be
        {
            read_records2 = 0;
            int j = first_buffer_size;  //the first position of the input buffer
            int sum2 = 0;
            while (!input_file2.eof() && j < first_buffer_size + second_buffer_size) {
                input_file2.read(reinterpret_cast<char*> (&buffer[j]), sizeof (block_t));
                sum2 += buffer[j].nreserved;
                if (input_file2.eof()){
                    if(j==first_buffer_size)
                    {
                        input2Done=1;   //if the input finished earlier but it was not recognized by eof()
                    }
                    else
                    {
                        input2Done=2;   //if the input has finished now

                    }
                }
                j++;
            }
            read_records2 = sum2;
            (*nios)++;
            used_blocks2 = (read_records2 / MAX_RECORDS_PER_BLOCK);
            if (read_records2 % MAX_RECORDS_PER_BLOCK != 0) used_blocks2++;
            mustToRead2 = false;
        }

        if(input1Done==1 || input2Done==1) break;

        if(input1Done==2 || input2Done==2) oneInputDone=true;

        bool allParsed = true; //This variable becomes false when all the records of one memory buffer were parsed.

        while (allParsed)
        {

            int compare;
            record_t *record = firstBufPtr;
            record_t *record2=secondBufPtr;
            //We compare the records
            if (field == 0) {
                if (record->recid == record2->recid) compare = 0;
                else if (record->recid > record2->recid) compare = 1;
                else compare = -1;
            } else if (field == 1) {
                if (record->num == record2->num) compare = 0;
                else if (record->num > record2->num) compare = 1;
                else compare = -1;
            } else if (field == 2) {
                if (strcmp(record->str, record2->str) == 0) compare = 0;
                else if (strcmp(record->str, record2->str) > 0) compare = 1;
                else compare = -1;
            } else if (field == 3) {
                if (record->num == record2->num) {
                int strCR = strcmp(record->str, record2->str);
                if (strCR == 0) compare = 0;
                else if (strCR > 0) compare = 1;
                else compare = -1;
            } else if (record->num > record2->num) compare = 1;
            else compare = -1;
        }
            if (compare == 0) { //if the records are the same,
                                //copy the fields from the input part of the buffer to the output part of the buffer:
                outputBufPtr->num = firstBufPtr->num;
                outputBufPtr->recid = firstBufPtr->recid;
                strcpy(outputBufPtr->str, firstBufPtr->str);
                outputBufPtr->valid = firstBufPtr->valid;
                mustToWrite = true;
                writtenInOutBuf++;
                buffer[outputBufBlockCtr].nreserved++;

                (*nres)++;
                outputBufRecordCtr++;
                if (outputBufRecordCtr >= MAX_RECORDS_PER_BLOCK) {
                    outputBufRecordCtr = 0;
                    outputBufBlockCtr++;

                    if (outputBufBlockCtr >= output_buffer_size) {
                        outputBufBlockCtr = first_buffer_size+second_buffer_size;
                        unsigned int pos=first_buffer_size+second_buffer_size;//The variable position shows the position.
                        while(pos<nmem_blocks)
                        {
                            buffer[pos].blockid=ctr;
                            ctr++;
                            pos++;
                        }

                        //write to the output file
                        unsigned int firstOutputIndex = first_buffer_size+second_buffer_size;

                        //for every block in the output buffer
                        for (int i = firstOutputIndex; i < firstOutputIndex + output_buffer_size; i++) {
                            output_file.write(reinterpret_cast<const char*> (&buffer[i]), sizeof (block_t));
                        }
                        (*nios)++;

                        writtenInOutBuf==0;

                        pos=first_buffer_size+second_buffer_size;

                        while(pos<nmem_blocks)
                        {
                            buffer[pos].nreserved = 0;
                            for(int j=0;j<MAX_RECORDS_PER_BLOCK;j++)
                            {
                                buffer[pos].entries[j].valid=false;
                            }
                            pos++;
                        }
                        mustToWrite = false;
                    }
                }
                outputBufPtr = &(buffer[outputBufBlockCtr].entries[outputBufRecordCtr]);

                firstBufRecordCtr++;
                secondBufRecordCtr++;
            }
            else if(compare>0)      //else if buffer1 record is bigger
            {
                secondBufRecordCtr++;
            }
            else    //else
            {
                firstBufRecordCtr++;
            }
            if(firstBufRecordCtr >= buffer[firstBufBlockCtr].nreserved)
            {
                firstBufRecordCtr=0;
                firstBufBlockCtr++;
                if(firstBufBlockCtr >= used_blocks1)
                {
                    firstBufBlockCtr = 0;
                    allParsed=false;
                    mustToRead1=true;
                }
            }
            if(secondBufRecordCtr >= buffer[secondBufBlockCtr].nreserved)
            {
                secondBufRecordCtr=0;
                secondBufBlockCtr++;
                if(secondBufBlockCtr-first_buffer_size >= used_blocks2)
                {
                    secondBufBlockCtr = first_buffer_size;
                    allParsed=false;
                    mustToRead2=true;
                }
            }
            firstBufPtr = &(buffer[firstBufBlockCtr].entries[firstBufRecordCtr]);
            secondBufPtr = &(buffer[secondBufBlockCtr].entries[secondBufRecordCtr]);
        }
    }
   if (mustToWrite) {
        unsigned int blocksUsed = (writtenInOutBuf / MAX_RECORDS_PER_BLOCK);
        if (writtenInOutBuf % MAX_RECORDS_PER_BLOCK != 0) blocksUsed++;
        //write to the output file
        unsigned int firstOutputIndex2 = first_buffer_size + second_buffer_size;

        //for every block in the output buffer
        for (int i = firstOutputIndex2; i < firstOutputIndex2 + blocksUsed; i++) {
            output_file.write(reinterpret_cast<const char*> (&buffer[i]), sizeof (block_t));
        }
        (*nios)++;
    }
    input_file1.close();
    input_file2.close();
    output_file.close();
    remove(tmpFile1);
    remove(tmpFile2);

}

