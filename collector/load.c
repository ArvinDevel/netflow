/*************************************************************************
        > File Name: load.c
        > Author: 
        > Mail: 
        > Created Time: Wed 20 May 2015 08:06:54 AM PDT
 ************************************************************************/
#include "load.h"
#include "conf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

//***************************************************
static void checkSpace();

//***************************************************
//      Global function
//***************************************************

void test_loadData() {
    testDataList.datalist = (testData*) malloc(sizeof (testData) * BASEINC);
    if (testDataList.datalist == NULL) {
        printf("Malloc %lu space for test fail!\n", sizeof (testData) * BASEINC);
        exit(-1);
    }

    char* files[3];
    files[0] = netflowtest.testLoadTemp; //load template first
    files[1] = netflowtest.testLoadMix;
    files[2] = netflowtest.testLoadData;

    int i = 0;
    short length = 0;
    for (; i < 3; i++) {
        if (strlen(files[i]) == 0) {
            continue;
        }
        FILE* fp = fopen(files[i], "rb");
        if (fp == NULL) {
            printf("Can not open file %s, %s\n", files[i], strerror(errno));
            continue;
        }

        while (feof(fp) == 0) {
            // read the data
            checkSpace();

            fread(&length, sizeof (short), 1, fp); // length dees not include itself
            length = ntohs(length); //  length
            if (length > 1480 || length <= 0) {
                // skip the data
                continue;
            }

            testData* p = testDataList.datalist + testDataList.totalNum;
            fread(p->data, sizeof (char), length, fp);
            p->length = length;
            testDataList.totalNum++;
        }
        fclose(fp);
    }

    // vertify
    if (testDataList.totalNum == 0) {
        printf("Can not find test data file, please check again!\n");
        exit(-1);
    } else {
        printf("Load test data ok!!\n");
    }
}

testData* getData() {
    testData* data = testDataList.datalist + testDataList.currId;
    unsigned int time = *((unsigned int*) (data->data + 8));
    unsigned int h_time = ntohl(time);
    h_time + testDataList.cycleCount * 10 * 60 * 1000;
    time = htonl(h_time);
    memcpy(data->data + 8, &time, sizeof (unsigned int));

    testDataList.currId++;
    if (testDataList.currId == testDataList.totalNum) {
        testDataList.cycleCount++;
        testDataList.currId = 0;
    }
    return data;
}

//***************************************************
//      personal function
//***************************************************

static void checkSpace() {
    if (testDataList.totalNum == testDataList.maxNum) {
        testDataList.datalist =
                (testData*) realloc(testDataList.datalist,
                testDataList.maxNum + sizeof (testData) * BASEINC);
        if (testDataList.datalist != NULL) {
            testDataList.maxNum += BASEINC;
        }
    }
}