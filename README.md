# DB-QueryProcessingAlgorithms
基于ExtMem程序库，用C语言模拟实现数据库中的几种查询处理算法  
### ExtMem程序库  
* 由C语言开发
* 模拟内存缓冲区管理和磁盘块读/写
* 1个数据结构：Buffer，它包含如下6个域：
  * numIO：外存 I/O 次数
  * bufSize：缓冲区大小（单位：字节）
  * blkSize：块的大小（单位：字节）
  * numAllBlk：缓冲区内可存放的最多块数
  * numFreeBlk：缓冲区内可用的块数
  * data：缓冲区内存区域
* 7个API函数：
  * initBuffer：初始化缓冲区buf
  * getNewBlockInBuffer：在缓冲区中申请一个新的块
  * readBlockFromDisk：将磁盘上地址为addr的磁盘块读入缓冲区buf
  * writeBlockToDisk：将缓冲区buf内的块blk写入磁盘上地址为addr的磁盘块
  * freeBlockInBuffer：解除块对缓冲区内存的占用
  * dropBlockOnDisk：从磁盘上删除地址为addr的磁盘块内的数据
  * freeBuffer：释放缓冲区buf占用的内存空间

*文件 test.c 中给出了 ExtMem 库使用方法的具体示例*
### 实现的查询处理算法  
1.基于线性搜索的关系选择算法  
2.两阶段多路归并排序算法（TPMMS）  
3.基于索引的关系选择算法  
4.基于排序的连接操作算法（Sort-Merge-Join）  
5.基于排序的两趟扫描算法（交、并、差）
