#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "extmem.h"

#define BLKNUM_R 16
#define BLKNUM_S 32
#define BUFSIZE 520
#define BLKSIZE 64
#define R_START 1
#define S_START 17

/**
 * 获取块中指定元组的字段值
 */
void getValueFromTuple(unsigned char *blk, int row, int *v1, int *v2) {
    char str[5];
    int i;
    for(i = 0; i < 4; i++) {
        str[i] = *(blk + 8*row + i);
    }
    *v1 = atoi(str);
    for(i = 0; i < 4; i++) {
        str[i] = *(blk + 8*row + 4 + i);
    }
    *v2 = atoi(str);
}

/**
 * 按给定字段值向块中写入元组
 */
void writeTupleToBlock(unsigned char *blk, int row, int v1, int v2) {
    char str[5];
    itoa(v1, str, 10);
    strcpy((char*)blk + 8*row, str);
    itoa(v2, str, 10);
    strcpy((char*)blk + 8*row + 4, str);
}

/**
 * 将下一个磁盘块的地址写入块中
 */
void writeNextAddrToBlock(unsigned char *blk, int addr) {
    char str[8];
    itoa(addr, str, 10);
    strcpy((char*)blk + 56, str);
}

/**
 * 获取下一个磁盘块的块号
 */
int getNextBlockAddr(unsigned char *blk) {
    char str[8];
    strcpy(str, (char*)blk + 56);
    return atoi(str);
}

/**
 * 交换两个元组的位置
 */
void swap(unsigned char *tuple1, unsigned char *tuple2) {
    char temp[9];
    memcpy(temp, tuple1, 8);
    memcpy(tuple1, tuple2, 8);
    memcpy(tuple2, temp, 8);
}

/**
 * 对多个块中的元组进行排序（冒泡排序，升序）
 */
void sortTuples(unsigned char** blk_array, int para_no, int n) {
    int para[3];
    unsigned char *blk1, *blk2;
    int i, j, flag;
    for(i = 0; i < 7*n - 1; i++) {
        flag = 0;
        for(j = 0; j < 7*n - i - 1; j++) {
            blk1 = blk_array[j/7];
            blk2 = blk_array[(j+1)/7];
            getValueFromTuple(blk1, j%7, &para[0], &para[1]);
            para[0] = para[para_no - 1];
            getValueFromTuple(blk2, (j+1)%7, &para[1], &para[2]);
            para[1] = para[para_no];
            if(para[0] > para[1]) {
                swap(blk1 + 8*(j%7), blk2 + 8*((j+1)%7));
                flag = 1;
            }
        }
        if(flag == 0) {
            break;
        }
    }
}

/**
 * 找出块中指定字段值最小的元组在块中的位置
 */
int findMin(unsigned char *blk, int para_no, int n) {
    int para[2];
    int i, min, p = -1;
    min = 401;
    for(i = 0; i < n; i++) {
        getValueFromTuple(blk, i, &para[0], &para[1]);
        if(min > para[para_no - 1] && para[para_no - 1] > 0) {
            min = para[para_no - 1];
            p = i;
        }
    }
    return p;
}

/**
 * 基于线性搜索的关系选择算法
 * 模拟实现 select S.C, S.D from S where S.C = value
 * value：要搜索的字段值
 * disk_write：结果要写入的首个磁盘块
 */
int line_search_relation_select(int value, int disk_write) {
    Buffer buf;
    unsigned char *blk_r, *blk_w;
    int i, j;
    int addr_r = S_START, addr_w = disk_write;
    int SC, SD;
    int num = 0;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    for(i = 0; i < BLKNUM_S; i++) {  //遍历关系S对应的磁盘块
        if((blk_r = readBlockFromDisk(addr_r, &buf)) == NULL) {
            perror("Reading Block Failed!\n");
            return -1;
        }
        printf("读入数据块%d\n", addr_r);
        for(j = 0; j < 7; j++) {  //遍历块中的每一个元组
            getValueFromTuple(blk_r, j, &SC, &SD);
            if(SC == value) {
                printf("(S.C = %d, S.D = %d)\n", SC, SD);
                writeTupleToBlock(blk_w, num%7, SC, SD);
                num++;
                if(num % 7 == 0) {  //内存块写满7个元组
                    writeNextAddrToBlock(blk_w, addr_w+1);
                    if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
                    printf("注：结果写入磁盘：%d\n", addr_w);
                    addr_w++;
                    blk_w = getNewBlockInBuffer(&buf);
                    memset(blk_w, 0, BLKSIZE);
                }
            }
        }
        addr_r = getNextBlockAddr(blk_r);
        freeBlockInBuffer(blk_r, &buf);
    }

    if(num % 7 != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }
    printf("\n满足选择条件的元组一共%d个\n\n", num);
    printf("IO读写一共%ld次\n", buf.numIO);

    freeBuffer(&buf);
    return 0;
}

/**
 * 对关系进行子表划分及子表排序
 * disk_start：待排序关系存放的首个磁盘块
 * num：待排序关系所占的磁盘块的总数
 * para_no：要排序的字段（para_no=1表示按关系的第一个字段值进行排序，para_no=2表示按关系的第二个字段值进行排序）
 * disk_write：结果要写入的首个磁盘块
 * head：存放排完序后各个子集合的首地址
 */
int devide_and_sort(int disk_start, int num, int para_no, int disk_write, int *head) {
    Buffer buf;
    unsigned char *blk_r[7];  //1个子集合中有7个数据块
    int addr_r = disk_start, addr_w = disk_write;
    int m, n, i, j;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    m = num % 7 == 0 ? num / 7 : num / 7 + 1;

    for(i = 0; i < m; i++) {
        n = i < m-1 ? 7 : num - 7*(m-1);
        if(head != NULL) {
            head[i] = addr_w;
        }
        for(j = 0; j < n; j++) {
            if((blk_r[j] = readBlockFromDisk(addr_r, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            addr_r = getNextBlockAddr(blk_r[j]);
        }
        sortTuples(blk_r, para_no, n);  //内排序
        for(j = 0; j < n; j++) {
            writeNextAddrToBlock(blk_r[j], addr_w+1);
            if(writeBlockToDisk(blk_r[j], addr_w, &buf) != 0) {
                perror("Writing Block Failed!\n");
                return -1;
            }
            addr_w++;
        }
    }

    freeBuffer(&buf);
    return 0;
}

/**
 * 两阶段多路归并排序算法
 * disk_start：待排序关系存放的首个磁盘块
 * num：待排序关系所占的磁盘块的总数
 * para_no：要排序的字段（para_no=1表示按关系的第一个字段值进行排序，para_no=2表示按关系的第二个字段值进行排序）
 * disk_write：结果要写入的首个磁盘块
 */
int TPMMS(int disk_start, int num, int para_no, int disk_write) {
    Buffer buf;
    unsigned char **blk_r;
    unsigned char *blk_cmp, *blk_w;
    int addr_r, addr_w;
    int m, n, i, p;
    int *head, *row;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    m = num % 7 == 0 ? num / 7 : num / 7 + 1;
    head = (int *)malloc(m * sizeof(int));
    row = (int *)malloc(m * sizeof(int));

    devide_and_sort(disk_start, num, para_no, disk_write + 100, head);

    blk_r = (unsigned char **)malloc(m * sizeof(unsigned char *));
    blk_cmp = getNewBlockInBuffer(&buf);
    blk_w = getNewBlockInBuffer(&buf);
    addr_w = disk_write;
    for(i = 0; i < m; i++) {
        addr_r = head[i];
        if((blk_r[i] = readBlockFromDisk(addr_r, &buf)) == NULL) {
            perror("Reading Block Failed!\n");
            return -1;
        }
        memcpy(blk_cmp + 8*i, blk_r[i], 8);
        row[i] = 0;  //记录各子集合中当前数据块内正参与归并的元组
    }
    n = 0;
    while((p = findMin(blk_cmp, para_no, m)) >= 0) {
        memcpy(blk_w + 8*n, blk_cmp + 8*p, 8);
        n++;
        if(n == 7) {  //内存块写满7个元组
            writeNextAddrToBlock(blk_w, addr_w+1);
            if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                perror("Writing Block Failed!\n");
                return -1;
            }
            printf("注：结果写入磁盘：%d\n", addr_w);
            addr_w++;
            blk_w = getNewBlockInBuffer(&buf);
            memset(blk_w, 0, BLKSIZE);
            n = 0;
        }
        row[p]++;
        if(row[p] < 7 && *(blk_r[p] + row[p]*8)) {
            memcpy(blk_cmp + p*8, blk_r[p] + row[p]*8, 8);
        }
        else {
            freeBlockInBuffer(blk_r[p], &buf);
            head[p]++;
            if((head[p] - disk_write - 100) % 7 != 0 && (head[p] - disk_write - 100) != num) {
                if((blk_r[p] = readBlockFromDisk(head[p], &buf)) == NULL) {
                    perror("Reading Block Failed!\n");
                    return -1;
                }
                memcpy(blk_cmp + 8*p, blk_r[p], 8);
                row[p] = 0;
            }
            else {  //下标p对应的子集合中没有下一磁盘块
                blk_r[p] = NULL;
                memset(blk_cmp + 8*p, 0, 8);
            }
        }
    }

    if(n != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    printf("两阶段多路归并排序完毕！\n");

    freeBlockInBuffer(blk_cmp, &buf);
    free(blk_r);
    free(head);
    free(row);
    freeBuffer(&buf);

    return 0;
}

/**
 * 创建索引
 * disk_start：待处理关系存放的首个磁盘块
 * num：待处理关系所占的磁盘块的总数
 * para_no：要建立索引的字段（para_no=1表示按关系的第一个字段建立索引，para_no=2表示按关系的第二个字段建立索引）
 * disk_write：索引要写入的首个磁盘块
 */
int createIndex(int disk_start, int num, int para_no, int disk_write) {
    Buffer buf;
    unsigned char *blk_r, *blk_w;
    int addr_r = disk_start, addr_w = disk_write;
    int i, n = 0;
    int para[2];

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    for(i = 0; i < num; i++) {
        if((blk_r = readBlockFromDisk(addr_r, &buf)) == NULL) {
            perror("Reading Block Failed!\n");
            return -1;
        }
        getValueFromTuple(blk_r, 0, &para[0], &para[1]);
        writeTupleToBlock(blk_w, n, para[para_no - 1], addr_r);
        n++;
        if(n == 7) {  //内存块写满7个元组
            writeNextAddrToBlock(blk_w, addr_w+1);
            if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                perror("Writing Block Failed!\n");
                return -1;
            }
            printf("注：索引写入磁盘：%d\n", addr_w);
            addr_w++;
            blk_w = getNewBlockInBuffer(&buf);
            memset(blk_w, 0, BLKSIZE);
            n = 0;
        }
        addr_r = getNextBlockAddr(blk_r);
        freeBlockInBuffer(blk_r, &buf);
    }

    if(n != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：索引写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    printf("索引建立完毕！\n");

    freeBuffer(&buf);
    return 0;
}

/**
 * 基于索引的关系选择算法
 * 模拟实现 select S.C, S.D from S where S.C = value
 * disk_idx：索引文件存放的首个磁盘块
 * value：要搜索的字段值
 * disk_write：结果要写入的首个磁盘块
 */
int index_relation_select(int disk_idx, int value, int disk_write) {
    Buffer buf;
    unsigned char *blk_idx, *blk_r, *blk_w;
    int addr_idx = disk_idx, addr_r = S_START, addr_w = disk_write;
    int i, j, num = 0;
    int SC, SD, idx, blk, idx_0, blk_0;
    int upd = 1;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    for(i = 0; i <= BLKNUM_S; i++) {  //遍历索引（有几个磁盘块就有几个索引）
        if(addr_idx != disk_idx + i/7) {
            addr_idx = disk_idx + i/7;
            upd = 1;
            freeBlockInBuffer(blk_idx, &buf);
        }
        if(upd == 1) {
            if((blk_idx = readBlockFromDisk(addr_idx, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            printf("读入索引块%d\n", addr_idx);
            upd = 0;
        }

        if(i == 0) {
            getValueFromTuple(blk_idx, i, &idx_0, &blk_0);
            continue;
        }

        if(i < BLKNUM_S) {
            getValueFromTuple(blk_idx, i, &idx, &blk);
        }
        else {
            idx = value + 1;  //额外遍历一次，以考察最后一个索引
        }

        if(idx_0 <= value && idx >= value) {
            addr_r = blk_0;
            if((blk_r = readBlockFromDisk(addr_r, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            printf("读入数据块%d\n", addr_r);
            for(j = 0; j < 7; j++) {  //遍历数据块中的每一个元组
                getValueFromTuple(blk_r, j, &SC, &SD);
                if(SC == value) {
                    printf("(S.C = %d, S.D = %d)\n", SC, SD);
                    writeTupleToBlock(blk_w, num%7, SC, SD);
                    num++;
                    if(num % 7 == 0) {  //内存块写满7个元组
                        writeNextAddrToBlock(blk_w, addr_w+1);
                        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                            perror("Writing Block Failed!\n");
                            return -1;
                        }
                        printf("注：结果写入磁盘：%d\n", addr_w);
                        addr_w++;
                        blk_w = getNewBlockInBuffer(&buf);
                        memset(blk_w, 0, BLKSIZE);
                    }
                }
            }
            freeBlockInBuffer(blk_r, &buf);
        }
        if(idx > value){
            break;
        }
        idx_0 = idx;
        blk_0 = blk;
    }

    freeBlockInBuffer(blk_idx, &buf);

    if(num % 7 != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    printf("\n满足选择条件的元组一共%d个\n\n", num);
    printf("IO读写一共%ld次\n", buf.numIO);

    freeBuffer(&buf);
    return 0;
}

/**
 * 基于排序的连接操作算法
 * 模拟实现 select S.C, S.D, R.A, R.B from S inner join R on S.C = R.A
 * disk_write：结果要写入的首个磁盘块
 */
int sort_merge_join(int disk_write) {
    Buffer buf;
    unsigned char *blk_r_R, *blk_r_S, *blk_w;
    int addr_r_R, addr_r_S, addr_w = disk_write;
    int t_R = 0, t_S = 0, temp;
    int RA, RB, SC, SD;
    int n = 0, cnt = 0;
    int flag = 0, upd_R = 1, upd_S = 1;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    //对关系R进行排序
    printf("对关系R进行归并排序（基于属性A）：\n");
    TPMMS(R_START, BLKNUM_R, 1, disk_write + 100);
    addr_r_R = disk_write + 100;

    //对关系S进行排序
    printf("对关系S进行归并排序（基于属性C）：\n");
    TPMMS(S_START, BLKNUM_S, 1, disk_write + BLKNUM_R + 100);
    addr_r_S = disk_write + BLKNUM_R + 100;
    printf("\n");

    SC = 0;
    while(SC <= 160) {
        if(upd_R == 1) {
            if((blk_r_R = readBlockFromDisk(addr_r_R, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_R = 0;
        }

        if(upd_S == 1) {
            if((blk_r_S = readBlockFromDisk(addr_r_S, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_S = 0;
        }

        getValueFromTuple(blk_r_R, t_R%7, &RA, &RB);  //t_R指向关系R中当前被读入的元组
        getValueFromTuple(blk_r_S, t_S%7, &SC, &SD);  //t_S指向关系S中当前被读入的元组

        if(RA == SC) {
            if(flag == 0) {
                temp = t_R;
                flag = 1;
            }
            cnt++;
            printf("(S.C = %d, S.D = %d, R.A = %d, R.B = %d)\n", SC, SD, RA, RB);
            writeTupleToBlock(blk_w, n%6, SC, SD);
            writeTupleToBlock(blk_w, ++n%6, RA, RB);
            n++;
            if(n % 6 == 0) {  //内存块写满6行（2行对应1个元组）
                writeNextAddrToBlock(blk_w, addr_w+1);
                if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                    perror("Writing Block Failed!\n");
                    return -1;
                }
                printf("注：结果写入磁盘：%d\n", addr_w);
                addr_w++;
                blk_w = getNewBlockInBuffer(&buf);
                memset(blk_w, 0, BLKSIZE);
            }
            t_R++;
            if(t_R == 7*BLKNUM_R) {
                t_R = temp;  //恢复
                flag = 0;
                t_S++;
            }
        }
        else if(RA < SC) {
            flag = 0;
            t_R++;
        }
        else {
            if(flag == 1) {
                t_R = temp;  //恢复
                flag = 0;
            }
            t_S++;
        }
        if(addr_r_R != disk_write + 100 + t_R/7) {
            addr_r_R = disk_write + 100 + t_R/7;
            if(addr_r_R >= disk_write + 100 + BLKNUM_R) {
                break;
            }
            upd_R = 1;
            freeBlockInBuffer(blk_r_R, &buf);
        }
        if(addr_r_S != disk_write + 100 + BLKNUM_R + t_S/7) {
            addr_r_S = disk_write + 100 + BLKNUM_R + t_S/7;
            if(addr_r_S >= disk_write + 100 + BLKNUM_R + BLKNUM_S) {
                break;
            }
            upd_S = 1;
            freeBlockInBuffer(blk_r_S, &buf);
        }
    }

    if(n % 6 != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    freeBlockInBuffer(blk_r_R, &buf);
    freeBlockInBuffer(blk_r_S, &buf);

    printf("\n总共连接%d次\n", cnt);

    freeBuffer(&buf);
    return 0;
}

/**
 * 基于排序的集合的交算法
 * disk_write：结果要写入的首个磁盘块
 */
int sort_intersect(int disk_write) {
    Buffer buf;
    unsigned char *blk_r_R, *blk_r_S, *blk_w;
    int addr_r_R, addr_r_S, addr_w = disk_write;
    int t_R = 0, t_S = 0, temp;
    int RA, RB, SC, SD;
    int num = 0;
    int flag = 0, upd_R = 1, upd_S = 1;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    //对关系R进行排序
    printf("对关系R进行归并排序（基于属性A）：\n");
    TPMMS(R_START, BLKNUM_R, 1, disk_write + 100);
    addr_r_R = disk_write + 100;

    //对关系S进行排序
    printf("对关系S进行归并排序（基于属性C）：\n");
    TPMMS(S_START, BLKNUM_S, 1, disk_write + BLKNUM_R + 100);
    addr_r_S = disk_write + BLKNUM_R + 100;
    printf("\n");

    SC = 0;
    while(SC <= 160) {
        if(upd_R == 1) {
            if((blk_r_R = readBlockFromDisk(addr_r_R, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_R = 0;
        }

        if(upd_S == 1) {
            if((blk_r_S = readBlockFromDisk(addr_r_S, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_S = 0;
        }

        getValueFromTuple(blk_r_R, t_R%7, &RA, &RB);  //t_R指向关系R中当前被读入的元组
        getValueFromTuple(blk_r_S, t_S%7, &SC, &SD);  //t_S指向关系S中当前被读入的元组

        if(RA == SC) {
            if(flag == 0) {
                temp = t_R;
                flag = 1;
            }
            if(RB == SD) {
                printf("(X = %d, Y = %d)\n", SC, SD);
                writeTupleToBlock(blk_w, num%7, SC, SD);
                num++;
                if(num % 7 == 0) {  //内存块写满7个元组
                    writeNextAddrToBlock(blk_w, addr_w+1);
                    if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
                    printf("注：结果写入磁盘：%d\n", addr_w);
                    addr_w++;
                    blk_w = getNewBlockInBuffer(&buf);
                    memset(blk_w, 0, BLKSIZE);
                }
                flag = 0;
                t_S++;
                t_R = temp - 1;
            }
            t_R++;
            if(t_R == 7*BLKNUM_R) {
                t_R = temp;  //恢复
                flag = 0;
                t_S++;
            }
        }
        else if(RA < SC) {
            flag = 0;
            t_R++;
        }
        else {
            if(flag == 1) {
                t_R = temp;  //恢复
                flag = 0;
            }
            t_S++;
        }
        if(addr_r_R != disk_write + 100 + t_R/7) {
            addr_r_R = disk_write + 100 + t_R/7;
            if(addr_r_R >= disk_write + 100 + BLKNUM_R) {
                break;
            }
            upd_R = 1;
            freeBlockInBuffer(blk_r_R, &buf);
        }
        if(addr_r_S != disk_write + 100 + BLKNUM_R + t_S/7) {
            addr_r_S = disk_write + 100 + BLKNUM_R + t_S/7;
            if(addr_r_S >= disk_write + 100 + BLKNUM_R + BLKNUM_S) {
                break;
            }
            upd_S = 1;
            freeBlockInBuffer(blk_r_S, &buf);
        }
    }

    if(num % 7 != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    freeBlockInBuffer(blk_r_R, &buf);
    freeBlockInBuffer(blk_r_S, &buf);

    printf("\nS和R的交集有%d个元组\n", num);

    freeBuffer(&buf);
    return 0;
}

/**
 * 基于排序的集合的并算法
 * disk_write：结果要写入的首个磁盘块
 */
int sort_union(int disk_write) {
    Buffer buf;
    unsigned char *blk_r_R, *blk_r_S, *blk_w;
    int addr_r_R, addr_r_S, addr_w = disk_write;
    int t_R = 0, t_S = 0, temp;
    int RA, RB, SC, SD;
    int num = 0;
    int flag = 0, upd_R = 1, upd_S = 1;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    //对关系R进行排序
    printf("对关系R进行归并排序（基于属性A）：\n");
    TPMMS(R_START, BLKNUM_R, 1, disk_write + 100);
    addr_r_R = disk_write + 100;

    //对关系S进行排序
    printf("对关系S进行归并排序（基于属性C）：\n");
    TPMMS(S_START, BLKNUM_S, 1, disk_write + BLKNUM_R + 100);
    addr_r_S = disk_write + BLKNUM_R + 100;
    printf("\n");

    while(1) {
        if(upd_R == 1) {
            if((blk_r_R = readBlockFromDisk(addr_r_R, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_R = 0;
        }

        if(upd_S == 1) {
            if((blk_r_S = readBlockFromDisk(addr_r_S, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_S = 0;
        }

        getValueFromTuple(blk_r_R, t_R%7, &RA, &RB);  //t_R指向关系R中当前被读入的元组
        getValueFromTuple(blk_r_S, t_S%7, &SC, &SD);  //t_S指向关系S中当前被读入的元组

        if(RA == SC) {
            if(flag == 0) {
                temp = t_R;
                flag = 1;
            }
            if(RB == SD) {
                flag = 0;
                t_S++;
                t_R = temp - 1;
            }
            t_R++;
            if(t_R == 7*BLKNUM_R) {
                t_R = temp;  //恢复
                flag = 0;
                printf("(X = %d, Y = %d)\n", SC, SD);
                writeTupleToBlock(blk_w, num%7, SC, SD);
                num++;
                if(num % 7 == 0) {  //内存块写满7个元组
                    writeNextAddrToBlock(blk_w, addr_w+1);
                    if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
                    printf("注：结果写入磁盘：%d\n", addr_w);
                    addr_w++;
                    blk_w = getNewBlockInBuffer(&buf);
                    memset(blk_w, 0, BLKSIZE);
                }
                t_S++;
            }
        }
        else if(RA < SC) {
            flag = 0;
            printf("(X = %d, Y = %d)\n", RA, RB);
            writeTupleToBlock(blk_w, num%7, RA, RB);
            num++;
            if(num % 7 == 0) {  //内存块写满7个元组
                writeNextAddrToBlock(blk_w, addr_w+1);
                if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                    perror("Writing Block Failed!\n");
                    return -1;
                }
                printf("注：结果写入磁盘：%d\n", addr_w);
                addr_w++;
                blk_w = getNewBlockInBuffer(&buf);
                memset(blk_w, 0, BLKSIZE);
            }
            t_R++;
        }
        else {
            if(flag == 1) {
                t_R = temp;  //恢复
                flag = 0;
            }
            printf("(X = %d, Y = %d)\n", SC, SD);
            writeTupleToBlock(blk_w, num%7, SC, SD);
            num++;
            if(num % 7 == 0) {  //内存块写满7个元组
                writeNextAddrToBlock(blk_w, addr_w+1);
                if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                    perror("Writing Block Failed!\n");
                    return -1;
                }
                printf("注：结果写入磁盘：%d\n", addr_w);
                addr_w++;
                blk_w = getNewBlockInBuffer(&buf);
                memset(blk_w, 0, BLKSIZE);
            }
            t_S++;
        }
        if(addr_r_R != disk_write + 100 + t_R/7) {
            addr_r_R = disk_write + 100 + t_R/7;
            if(addr_r_R >= disk_write + 100 + BLKNUM_R) {
                break;
            }
            upd_R = 1;
            freeBlockInBuffer(blk_r_R, &buf);
        }
        if(addr_r_S != disk_write + 100 + BLKNUM_R + t_S/7) {
            addr_r_S = disk_write + 100 + BLKNUM_R + t_S/7;
            if(addr_r_S >= disk_write + 100 + BLKNUM_R + BLKNUM_S) {
                break;
            }
            upd_S = 1;
            freeBlockInBuffer(blk_r_S, &buf);
        }
    }
    while(t_R < 7 * BLKNUM_R) {
        if(upd_R == 1) {
            if((blk_r_R = readBlockFromDisk(addr_r_R, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_R = 0;
        }
        getValueFromTuple(blk_r_R, t_R%7, &RA, &RB);

        printf("(X = %d, Y = %d)\n", RA, RB);
        writeTupleToBlock(blk_w, num%7, RA, RB);
        num++;
        if(num % 7 == 0) {  //内存块写满7个元组
            writeNextAddrToBlock(blk_w, addr_w+1);
            if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                perror("Writing Block Failed!\n");
                return -1;
            }
            printf("注：结果写入磁盘：%d\n", addr_w);
            addr_w++;
            blk_w = getNewBlockInBuffer(&buf);
            memset(blk_w, 0, BLKSIZE);
        }
        t_R++;

        if(addr_r_R != disk_write + 100 + t_R/7) {
            addr_r_R = disk_write + 100 + t_R/7;
            if(addr_r_R >= disk_write + 100 + BLKNUM_R) {
                break;
            }
            upd_R = 1;
            freeBlockInBuffer(blk_r_R, &buf);
        }
    }

    while(t_S < 7 * BLKNUM_S) {
        if(upd_S == 1) {
            if((blk_r_S = readBlockFromDisk(addr_r_S, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_S = 0;
        }
        getValueFromTuple(blk_r_S, t_S%7, &SC, &SD);

        printf("(X = %d, Y = %d)\n", SC, SD);
        writeTupleToBlock(blk_w, num%7, SC, SD);
        num++;
        if(num % 7 == 0) {  //内存块写满7个元组
            writeNextAddrToBlock(blk_w, addr_w+1);
            if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                perror("Writing Block Failed!\n");
                return -1;
            }
            printf("注：结果写入磁盘：%d\n", addr_w);
            addr_w++;
            blk_w = getNewBlockInBuffer(&buf);
            memset(blk_w, 0, BLKSIZE);
        }
        t_S++;

        if(addr_r_S != disk_write + 100 + BLKNUM_R + t_S/7) {
            addr_r_S = disk_write + 100 + BLKNUM_R + t_S/7;
            if(addr_r_S >= disk_write + 100 + BLKNUM_R + BLKNUM_S) {
                break;
            }
            upd_S = 1;
            freeBlockInBuffer(blk_r_S, &buf);
        }
    }

    if(num % 7 != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    freeBlockInBuffer(blk_r_R, &buf);
    freeBlockInBuffer(blk_r_S, &buf);

    printf("\nS和R的并集有%d个元组\n", num);

    freeBuffer(&buf);
    return 0;
}

/**
 * 基于排序的集合的差算法（S-R）
 * disk_write：结果要写入的首个磁盘块
 */
int sort_difference(int disk_write) {
    Buffer buf;
    unsigned char *blk_r_R, *blk_r_S, *blk_w;
    int addr_r_R, addr_r_S, addr_w = disk_write;
    int t_R = 0, t_S = 0, temp;
    int RA, RB, SC, SD;
    int num = 0;
    int flag = 0, upd_R = 1, upd_S = 1;

    if(!initBuffer(BUFSIZE, BLKSIZE, &buf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    blk_w = getNewBlockInBuffer(&buf);

    //对关系R进行排序
    printf("对关系R进行归并排序（基于属性A）：\n");
    TPMMS(R_START, BLKNUM_R, 1, disk_write + 100);
    addr_r_R = disk_write + 100;

    //对关系S进行排序
    printf("对关系S进行归并排序（基于属性C）：\n");
    TPMMS(S_START, BLKNUM_S, 1, disk_write + BLKNUM_R + 100);
    addr_r_S = disk_write + BLKNUM_R + 100;
    printf("\n");

    SC = 0;
    while(SC <= 160) {
        if(upd_R == 1) {
            if((blk_r_R = readBlockFromDisk(addr_r_R, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_R = 0;
        }

        if(upd_S == 1) {
            if((blk_r_S = readBlockFromDisk(addr_r_S, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_S = 0;
        }

        getValueFromTuple(blk_r_R, t_R%7, &RA, &RB);  //t_R指向关系R中当前被读入的元组
        getValueFromTuple(blk_r_S, t_S%7, &SC, &SD);  //t_S指向关系S中当前被读入的元组

        if(RA == SC) {
            if(flag == 0) {
                temp = t_R;
                flag = 1;
            }
            if(RB == SD) {
                flag = 0;
                t_S++;
                t_R = temp - 1;
            }
            t_R++;
            if(t_R == 7*BLKNUM_R) {
                t_R = temp;  //恢复
                flag = 0;
                printf("(X = %d, Y = %d)\n", SC, SD);
                writeTupleToBlock(blk_w, num%7, SC, SD);
                num++;
                if(num % 7 == 0) {  //内存块写满7个元组
                    writeNextAddrToBlock(blk_w, addr_w+1);
                    if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
                    printf("注：结果写入磁盘：%d\n", addr_w);
                    addr_w++;
                    blk_w = getNewBlockInBuffer(&buf);
                    memset(blk_w, 0, BLKSIZE);
                }
                t_S++;
            }
        }
        else if(RA < SC) {
            flag = 0;
            t_R++;
        }
        else {
            if(flag == 1) {
                t_R = temp;  //恢复
                flag = 0;
            }
            printf("(X = %d, Y = %d)\n", SC, SD);
            writeTupleToBlock(blk_w, num%7, SC, SD);
            num++;
            if(num % 7 == 0) {  //内存块写满7个元组
                writeNextAddrToBlock(blk_w, addr_w+1);
                if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                    perror("Writing Block Failed!\n");
                    return -1;
                }
                printf("注：结果写入磁盘：%d\n", addr_w);
                addr_w++;
                blk_w = getNewBlockInBuffer(&buf);
                memset(blk_w, 0, BLKSIZE);
            }
            t_S++;
        }
        if(addr_r_R != disk_write + 100 + t_R/7) {
            addr_r_R = disk_write + 100 + t_R/7;
            if(addr_r_R >= disk_write + 100 + BLKNUM_R) {
                break;
            }
            upd_R = 1;
            freeBlockInBuffer(blk_r_R, &buf);
        }
        if(addr_r_S != disk_write + 100 + BLKNUM_R + t_S/7) {
            addr_r_S = disk_write + 100 + BLKNUM_R + t_S/7;
            if(addr_r_S >= disk_write + 100 + BLKNUM_R + BLKNUM_S) {
                break;
            }
            upd_S = 1;
            freeBlockInBuffer(blk_r_S, &buf);
        }
    }

    while(t_S < 7 * BLKNUM_S) {
        if(upd_S == 1) {
            if((blk_r_S = readBlockFromDisk(addr_r_S, &buf)) == NULL) {
                perror("Reading Block Failed!\n");
                return -1;
            }
            upd_S = 0;
        }
        getValueFromTuple(blk_r_S, t_S%7, &SC, &SD);

        printf("(X = %d, Y = %d)\n", SC, SD);
        writeTupleToBlock(blk_w, num%7, SC, SD);
        num++;
        if(num % 7 == 0) {  //内存块写满7个元组
            writeNextAddrToBlock(blk_w, addr_w+1);
            if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
                perror("Writing Block Failed!\n");
                return -1;
            }
            printf("注：结果写入磁盘：%d\n", addr_w);
            addr_w++;
            blk_w = getNewBlockInBuffer(&buf);
            memset(blk_w, 0, BLKSIZE);
        }
        t_S++;

        if(addr_r_S != disk_write + 100 + BLKNUM_R + t_S/7) {
            addr_r_S = disk_write + 100 + BLKNUM_R + t_S/7;
            if(addr_r_S >= disk_write + 100 + BLKNUM_R + BLKNUM_S) {
                break;
            }
            upd_S = 1;
            freeBlockInBuffer(blk_r_S, &buf);
        }
    }

    if(num % 7 != 0) {
        writeNextAddrToBlock(blk_w, addr_w+1);
        if(writeBlockToDisk(blk_w, addr_w, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        printf("注：结果写入磁盘：%d\n", addr_w);
    }
    else {
        freeBlockInBuffer(blk_w, &buf);
    }

    freeBlockInBuffer(blk_r_R, &buf);
    freeBlockInBuffer(blk_r_S, &buf);

    printf("\nS和R的差集（S-R）有%d个元组\n", num);

    freeBuffer(&buf);
    return 0;
}


int main() {

    //任务一
    printf("\n------------------------------------------\n");
    printf("基于线性搜索的关系选择算法 S.C = 107：\n");
    printf("------------------------------------------\n");

    line_search_relation_select(107, 100);

    //任务二
    printf("\n------------------------------------------\n");
    printf("两阶段多路归并排序算法 R.A：\n");
    printf("------------------------------------------\n");

    TPMMS(R_START, BLKNUM_R, 1, 301);

    printf("\n------------------------------------------\n");
    printf("两阶段多路归并排序算法 S.C：\n");
    printf("------------------------------------------\n");

    TPMMS(S_START, BLKNUM_S, 1, 317);

    //任务三
    printf("\n------------------------------------------\n");
    printf("为关系S（已排序）中的属性C建立索引：\n");
    printf("------------------------------------------\n");

    createIndex(317, BLKNUM_S, 1, 350);

    printf("\n------------------------------------------\n");
    printf("基于索引的关系选择算法 S.C = 107：\n");
    printf("------------------------------------------\n");

    index_relation_select(350, 107, 120);

    //任务四
    printf("\n------------------------------------------\n");
    printf("基于排序的连接算法：\n");
    printf("------------------------------------------\n");

    sort_merge_join(500);

    //任务五
    printf("\n------------------------------------------\n");
    printf("基于排序的集合的交算法：\n");
    printf("------------------------------------------\n");

    sort_intersect(140);

    //附加题
    printf("\n------------------------------------------\n");
    printf("基于排序的集合的并算法：\n");
    printf("------------------------------------------\n");

    sort_union(700);

    printf("\n------------------------------------------\n");
    printf("基于排序的集合的差算法：\n");
    printf("------------------------------------------\n");

    sort_difference(900);

}
