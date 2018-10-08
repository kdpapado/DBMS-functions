#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "dbtproj.h"
using namespace std;

char* randStringGenerator() {   // It produces a random string
    char* rand_string;
    rand_string = (char*) malloc(STR_LENGTH);

    const char text[] = "abcdefghijklmnopqrstuvwxyz";
    int i;
    int length = rand() % (STR_LENGTH - 1);
    length += 1;
    for (i = 0; i < length; ++i) {
        rand_string[i] = text[rand() % (sizeof (text) - 1)];
    }
    rand_string[i] = '\0';
    return rand_string;
}

void produce(char *file, unsigned int size) {    // It produces a random file
    int outFile = creat(file, S_IRWXU);
    unsigned int recid = 0;
    record_t record;
    block_t block;
    for (unsigned int b = 0; b < size; ++b) { // for each block
        block.blockid = b;
        for (int r = 0; r < MAX_RECORDS_PER_BLOCK; ++r) { // for each record
            // Prepare a record:
            record.recid = recid += 1;
            record.num = rand() % 100000;
            strcpy(record.str, randStringGenerator()); // Put a random string to the record.
            if (((double) rand() / (RAND_MAX)) >= 0.02) {
                record.valid = true;
            } else {
                record.valid = false;
            }
            memcpy(&block.entries[r], &record, sizeof (record_t)); // Copy the record to the block.
        }
        block.nreserved = MAX_RECORDS_PER_BLOCK;
        block.valid = true;
        write(outFile, &block, sizeof (block_t));
    }
    close(outFile);
}

void produceTwoFiles(char *filename1, unsigned int size1, char *filename2, unsigned int size2) {
    // It produces two input files.
    srand(time(NULL));
    produce(filename1, size1);
    produce(filename2, size2);
}

int main()
{
    clock_t start, stop;
    double t = 0.0;
    char input1[] = "input1.bin";
    char input2[] = "input2.bin";
    char output[] = "output.bin";

    //76=~1MB, 76000=~1GB
    produceTwoFiles(input1, 760, input2, 600);

    unsigned int memBlocks = 22;    //number of blocks in memory
    block_t *buffer = new block_t[memBlocks];
    unsigned int sortedSegs = 0;    //number of sorted segments produced
    unsigned int passes = 0;    //number of passes required for sorting
    unsigned int ios = 0;   //number of IOs performed
    unsigned int nres = 0;
    unsigned int nunique = 0;

    //call Mergesort
    start = clock();
    cout<<"MergeSort starts running...\n\n";
    MergeSort(input1, 1, buffer, memBlocks, output, &sortedSegs, &passes, &ios);
    stop = clock();
    t = (double) (stop-start)/CLOCKS_PER_SEC;
    cout<<"Results:\n";
    cout<<"The number of sorted segments is "<<sortedSegs<< ".";
    cout<<"\nThe number of passes is "<<passes<<".";
    cout<<"\nThere were "<<ios<<" I/Os.";
    cout<<"\nThe runtime was "<<t<<".\n\n";

    //call EliminateDuplicates
    ios = 0;
    start = clock();
    cout<<"\nEliminate Duplicates starts running...\n\n";
    EliminateDuplicates(input1, 3, buffer, memBlocks, output, &nunique, &ios);
    stop = clock();
    t = (double) (stop-start)/CLOCKS_PER_SEC;
    cout<<"Results:\n";
    cout<<"The number of unique records is "<<nunique<<".";
    cout<<"\nThere were "<<ios<<" I/Os.";
    cout<<"\nThe runtime was "<<t<<".\n\n";

    //call MergeJoin
    ios = 0;
    start = clock();
    cout<<"\nMergeJoin starts running...\n\n";
    MergeJoin(input1, input2, 0, buffer, memBlocks, output, &nres, &ios);
    stop = clock();
    t = (double) (stop-start)/CLOCKS_PER_SEC;
    cout<<"Results:\n";
    cout<<"The number of pairs is: "<<nres<<".";
    cout<<"\nThere were "<<ios<<" I/Os.";
    cout<<"\nThe runtime was "<<t<<".\n\n";

    //call HashJoin
    ios = 0;
    nres = 0;
    start = clock();
    cout<<"\nHashjoin starts running...\n\n";
    HashJoin(input1, input2, 2, buffer, memBlocks, output, &nres, &ios);
    stop = clock();
    t = (double) (stop-start)/CLOCKS_PER_SEC;
    cout<<"Results:\n";
    cout<<"The number of pairs is: "<<nres<<".";
    cout<<"\nThere were "<<ios<<" I/Os.";
    cout<<"\nThe runtime was "<<t<<".\n\n";

    delete[] buffer;


    return 0;
}
