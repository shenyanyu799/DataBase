## 介绍
    这是一个简易的、能凑合着用的，本地数据管理程序，
    能够自动分配存储空间，并允许序列化检索数据，
    该程序适合用于应用存档，
    不同进程同时往一个文件内写入长度类似数据时可能出现问题，可以规避。

## 基本功能
1. 基于本地的数据序列化管理，实现类似数据库的功能
2. 能够~~快速~~查找数据、存储数据、增删数据
3. 实现多线程的同步、异步调用
+ DataList:
    - bool AllowRepeat { get; set; } => false允许存在同名key;true覆盖key
    - byte[] Read(string path, string key) => 读取key指向数据，如果key不存在返回null
    - string ReadText(string path, string key)
    - bool Write(string path, string key, byte[] data) => 写入key和data,如果AllowRepeat = true,则会检查key是否存在，并覆盖数据。
    - bool WriteText(string path, string key, string text)
    - int GetCount(string path) => 获取数据条目数量
    - void Remove(string path, string key) => 移除指定key
    - bool Contains(string path, string key) => 判断key是否存在
    - void Clear(string path) => 清除数据
    - string[] GetKeys(string path) => 获取所有key

## 设计思路

在本地文件存储基础上，对存储位置进行划区。
1. 按照数据条目进行划分，例如默认500条数据为一个区块。
2. 当一个区数据无法存放，即容量不足或者已经有500条数据时，创建一个新的区块。
3. 存储过程中不需要对区块内的每一条数据进行判断
    - 判断的参数为区块最大单独允许存放数据长度，如果该参数不满足则检索下一个区块。
    - 如果条件满足，则在当前区块分配空间，对当前区块索引进行序列化，然后分配空间，最后写入数据。
4. 可以对区块进行检索，然后获取该区块的索引表，索引直接指向数据的地址

## 实现功能
1. 该程序实现了，~~快速~~查找数据、存储数据、增删数据功能。
2. 无法实现高并发
    - 在不同进程中类似长度的数据可能在相同时间被分配至相同位置。
3. 分配空间速度影响
    - 受到区块最大数据条数影响。
    - 受到区块速度影响。
    - 受到CPU和磁盘读写速度影响。

