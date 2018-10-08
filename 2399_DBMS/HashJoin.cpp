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

long bufRecordSize; //The buffer's size for the records.

//This function returns the size of the file "file_name".
long returnFileSize(char *file_name)
{

    struct stat stat_buf;
    int rc = stat(file_name, &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * Function used for hashing process.
 * @param record
 * @return rValue
 */
int hashingHelper(unsigned char FIELD,record_t record)
{
    if (FIELD == 0 ) //hash for recid
    {
        return record.recid % bufRecordSize;
    }
    else if (FIELD == 1) //hash for num
    {
        return record.num % bufRecordSize;
    }
    else if (FIELD == 2) //hash for string
    {
        int sum = 0;
        int i = 0;
        while(record.str[i] != '\0')
        {
            sum += record.str[i];
            i++;
        }
        return sum % bufRecordSize;
    }
    else //hash for num and string
    {
        int sum = 0;
        int i = 0;
        while(record.str[i] != '\0')
        {
            sum += record.str[i];
            i++;
        }
        int rValue = (sum+record.num) % bufRecordSize;
        return rValue;
    }
}


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
void HashJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios)
{
    char file_name[100];

    //initialize the global variable bufRecordSize, which is used by the hash function
    bufRecordSize = (nmem_blocks-2) * MAX_RECORDS_PER_BLOCK;    //the last two blocks are reserved for the relation
                                                                //and the output

    //get the sizes of both files to find the smaller relation
    long size1 = returnFileSize(infile1);
    long size2 = returnFileSize(infile2);

    FILE *hashfile, *fileExplorer, *temp, *output;
    ofstream files;

    //open the smaller file in order to create the hash table
    if (size1 <= size2)
    {
        hashfile = fopen(infile1,"r");
        fileExplorer = fopen(infile2, "r");
    }
    else
    {
        hashfile = fopen(infile2,"r");
        fileExplorer = fopen(infile1, "r");
    }
    output = fopen(outfile, "w");   //open the output file

    //keep tracking of any files created for the hashjoin
    files.open("files.txt");

    int position;
    int out_i = 0;
    int id = 0;
    while(fread(&buffer[nmem_blocks-2], 1, sizeof (block_t), hashfile))
    {
        (*nios)++;
        for(int i=0; i<buffer[nmem_blocks-2].nreserved; i++)
        {
            //find the position of the record
            position = hashingHelper(field,buffer[nmem_blocks-2].entries[i]);
            if (!buffer[position/MAX_RECORDS_PER_BLOCK].entries[position%MAX_RECORDS_PER_BLOCK].valid)
            {//if the bucket is empty,add the matching record
                buffer[position/MAX_RECORDS_PER_BLOCK].entries[position%MAX_RECORDS_PER_BLOCK] = buffer[nmem_blocks-2].entries[i];
            }
            else
            {//if the bucket is full, add the record to the corresponding file
                sprintf(file_name, "file%d", position);
                if (returnFileSize(file_name) == -1)
                {//if the file does not exist,add it to the list of files created for the hash table
                    files<<file_name<<"\n";
                }
                temp  = fopen(file_name, "a");
                fwrite(&buffer[nmem_blocks-2].entries[i], 1, sizeof(record_t), temp);
                fclose(temp);
                (*nios)++;
            }
        }
    }
    fclose(hashfile);
    files.close();

    //The hash table and hash files are ready.
    //So, we will start reading the second relation:
    while(fread(&buffer[nmem_blocks-2], 1, sizeof(block_t), fileExplorer))
    {
        (*nios)++;
        for(int i=0; i<buffer[nmem_blocks-2].nreserved; i++)    //for every record in the block
        {

            position = hashingHelper(field,buffer[nmem_blocks-2].entries[i]);   //get their(of every record in the block) result
                                                                                //with the hash function.
            //get the matching bucket
            if (buffer[position/MAX_RECORDS_PER_BLOCK].entries[position%MAX_RECORDS_PER_BLOCK].valid)
            {//if the bucket is not empty,check the record
                bool write  = false;
                if(!compare(field,&(buffer[nmem_blocks-2].entries[i]), &(buffer[position/MAX_RECORDS_PER_BLOCK].entries[position%MAX_RECORDS_PER_BLOCK])))
                {//if the record in bucket matches,set the variable write to true in order to write the record to the output block
                    write = true;
                }
                else //if the records do not match,there may be another record matching in the file for this bucket
                {   //check if there is a file for this bucket
                    record_t tempRec;
                    sprintf(file_name, "file%d", position);
                    temp  = fopen(file_name, "r");
                    while(fread(&tempRec, 1, sizeof(record_t), temp))  //read one by one the records in it and try to find a match
                    {
                        (*nios)++;
                        if (!compare(field,&(buffer[nmem_blocks - 2].entries[i]), &tempRec))//if the record in the bucket matches,
                                                                                            //write to the output block
                        {
                            write = true;
                            break;
                        }
                    }
                    fclose(temp);
                }

                //write the record to the output
                if ( write )
                {
                    (*nres)++;
                    buffer[nmem_blocks - 1].entries[out_i] = buffer[nmem_blocks - 2].entries[i];
                    out_i++;
                    if ( out_i == MAX_RECORDS_PER_BLOCK )//if the output block is full,write to the file and continue
                    {
                        buffer[nmem_blocks - 1].blockid = id;
                        buffer[nmem_blocks - 1].nreserved = MAX_RECORDS_PER_BLOCK;
                        fwrite(&buffer[nmem_blocks - 1], 1, sizeof (block_t), output);
                        id++;
                        out_i = 0;
                        (*nios)++;
                    }
                }
            }
        }
    }

    //Now, check if there are records in the output block
    if ( out_i > 0 )
    {
        //write the last block to the output
        buffer[nmem_blocks - 1].blockid = id;
        buffer[nmem_blocks - 1].nreserved = MAX_RECORDS_PER_BLOCK;
        fwrite(&buffer[nmem_blocks - 1], 1, sizeof (block_t), output);
        (*nios)++;
    }

    //delete any files created for the hashing
    ifstream file_names;
    file_names.open("files.txt");
    while(!file_names.eof())
    {
        file_names >> file_name;
        remove(file_name);
    }
    file_names.close();
    remove("files.txt");
    fclose(fileExplorer);
    fclose(output);
}
