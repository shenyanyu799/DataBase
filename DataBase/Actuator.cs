using System;

namespace DataBase
{

    public class Actuator
    {
        
        private string fpath;
        private FileMode fmode;
        private FileAccess faccess;
        private FileShare fShare;
        //该参数不会写入文件
        protected internal int BlockCount = 500;
        public Actuator(string path, FileMode mode = FileMode.Open, FileAccess access = FileAccess.ReadWrite, FileShare fileShare = FileShare.ReadWrite)
        {
            //第一次打开流
            fpath = path;
            fmode = FileMode.Open;
            faccess = access;
            fShare = fileShare;
            //如果文件需要新建
            if(mode == FileMode.Create || !File.Exists(path))
            {
                using ( FileStream fileStream = new FileStream(path, mode, access, fileShare))
                {
                    fileStream.Write(Block.Default.ToBytes());
                    //fileStream.Position = 0;
                    //Console.WriteLine(Block.FormStream(fileStream));
                    //fileStream.Position += IToBytes.SizeOf<Pointer>() * BlockCount - 1;
                    //fileStream.WriteByte(0);
                    fileStream.Close();
                }
            }

        }
        /// <summary>
        /// 往指针位置写入数据
        /// </summary>
        /// <param name="pointer"></param>
        /// <returns></returns>
        public byte[] Read(Pointer pointer)
        {
            using ( FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare)) 
            {
                if(fileStream.Length < pointer.Index) return new byte[0];
                fileStream.Position = pointer.Index;
                byte[] buffer = new byte[pointer.Length];
                fileStream.Read(buffer, 0, buffer.Length);
                fileStream.Close();
                return buffer;
            }
        }
        /// <summary>
        /// 往指针位置写入数据
        /// </summary>
        /// <param name="pointer"></param>
        /// <returns></returns>
        public byte[] Read(Pointer pointer, int offs)
        {
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                if (fileStream.Length < pointer.Index + pointer.Length)
                {
                    return new byte[0];
                }
                if (pointer.Length - offs <= 0)
                {
                    return new byte[0];
                }
                fileStream.Position = pointer.Index + offs;
                byte[] buffer = new byte[pointer.Length - offs];
                fileStream.Read(buffer, 0, buffer.Length);
                fileStream.Close();
                return buffer;
            }
        }

        /// <summary>
        /// 自动分配并写入数据，返回写入后的指针
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public Pointer Write(ReadOnlySpan<byte> buffer)
        {
            Pointer rpointer = Calloc(buffer.Length);
            using ( FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                //写入数据
                fileStream.Position = rpointer.Index;
                fileStream.Write(buffer);
                fileStream.Close();
            }
            return rpointer;

        }
        private static bool IsCalloc;
        public Pointer Calloc(long size)
        {
            while (IsCalloc) Task.Delay(1).Wait();
            IsCalloc = true;
            Pointer rpointer = new Pointer();
            try
            {
                int bsize = IToBytes.SizeOf<Block>();
                int psize = IToBytes.SizeOf<Pointer>();
                long offs = 0;
                using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
                {
                    //读取区块
                    Block block1 = Block.FormStream(fileStream);
                    bool rw = true;
                    while (rw)
                    {
                        //当前区块可以存储
                        if (block1.Count < BlockCount && (block1.MaxSpace == -1 || block1.MaxSpace > size))
                        {
                            //分析已用空间
                            List<Pointer> pointers = new List<Pointer>();
                            if (block1.Count > 0)
                            {
                                for (int i = 0; i < BlockCount; i++)
                                {
                                    Pointer p = Pointer.FormStream(fileStream);
                                    if (p.Length > 0) pointers.Add(p);
                                }
                                pointers.Sort();
                                long max = block1.MaxSpace;

                                for (int i = 0; i < pointers.Count - 1; i++)
                                {
                                    long p = pointers[i].Index + pointers[i].Length;
                                    long l = pointers[i + 1].Index - p;
                                    if (l > size)
                                    {
                                        if (rpointer.Length == 0)
                                        {
                                            rpointer.Index = p;
                                            rpointer.Length = size;
                                        }
                                        if (l - size > max)
                                        {
                                            max = l;
                                        }
                                    }
                                    else
                                    {
                                        if (l > max)
                                        {
                                            max = l;
                                        }
                                    }
                                }
                                if (rpointer.Length == 0)
                                {
                                    rpointer.Index = pointers[^1].Index + pointers[^1].Length;
                                    rpointer.Length = size;
                                    if (block1.NextIndex != -1)
                                    {
                                        long l = block1.NextIndex - rpointer.Index - rpointer.Length;
                                        if (l > max)
                                        {
                                            max = l;
                                        }
                                    }
                                }
                                if (block1.NextIndex == -1)
                                {
                                    max = -1;
                                }
                                else
                                {
                                    block1.MaxSpace = (int)max;
                                }
                            }
                            else
                            {
                                rpointer.Index = offs + bsize + psize * BlockCount;
                                rpointer.Length = size;
                            }
                            //添加写入指针
                            pointers.Add(rpointer);
                            //添加数据块和索引表
                            block1.Count = pointers.Count;
                            while (pointers.Count < BlockCount)
                            {
                                pointers.Add(new Pointer());
                            }
                            //写入数据块
                            fileStream.Position = offs;
                            fileStream.Write(block1.ToBytes());
                            //写入索引表
                            foreach (Pointer item in pointers)
                            {
                                fileStream.Write(item.ToBytes());
                            }
                            rw = false;
                        }
                        else
                        {
                            //如果有下一个区块
                            if (block1.NextIndex != -1)
                            {
                                offs = block1.NextIndex;
                                fileStream.Position = offs;
                                block1 = Block.FormStream(fileStream);
                            }
                            else
                            {
                                //创建区块
                                Block block2 = Block.Default;
                                if (offs == 0)
                                    block2.LastIndex = -1;
                                else
                                    block2.LastIndex = offs;
                                block1.NextIndex = fileStream.Length;
                                //写入第一个区块数据
                                fileStream.Position = offs;
                                fileStream.Write(block1.ToBytes());
                                offs = block1.NextIndex;
                                block1 = block2;
                            }
                        }
                    }
                    fileStream.Close();
                }
            }
            catch
            {

            }
            IsCalloc = false;
            return rpointer;
        }
        public Pointer Calloc(FileStream fileStream, long size)
        {
            while (IsCalloc) Task.Delay(1).Wait();
            IsCalloc = true;
            Pointer rpointer = new Pointer();
            try
            {
                int bsize = IToBytes.SizeOf<Block>();
                int psize = IToBytes.SizeOf<Pointer>();
                long offs = 0;
                fileStream.Position = 0;
                {
                    //读取区块
                    Block block1 = Block.FormStream(fileStream);
                    bool rw = true;
                    while (rw)
                    {
                        //当前区块可以存储
                        if (block1.Count < BlockCount && (block1.MaxSpace == -1 || block1.MaxSpace > size))
                        {
                            //分析已用空间
                            List<Pointer> pointers = new List<Pointer>();
                            if (block1.Count > 0)
                            {
                                for (int i = 0; i < BlockCount; i++)
                                {
                                    Pointer p = Pointer.FormStream(fileStream);
                                    if (p.Length > 0) pointers.Add(p);
                                }
                                pointers.Sort();
                                long max = block1.MaxSpace;

                                for (int i = 0; i < pointers.Count - 1; i++)
                                {
                                    long p = pointers[i].Index + pointers[i].Length;
                                    long l = pointers[i + 1].Index - p;
                                    if (l > size)
                                    {
                                        if (rpointer.Length == 0)
                                        {
                                            rpointer.Index = p;
                                            rpointer.Length = size;
                                        }
                                        if (l - size > max)
                                        {
                                            max = l;
                                        }
                                    }
                                    else
                                    {
                                        if (l > max)
                                        {
                                            max = l;
                                        }
                                    }
                                }
                                if (rpointer.Length == 0)
                                {
                                    rpointer.Index = pointers[^1].Index + pointers[^1].Length;
                                    rpointer.Length = size;
                                    if (block1.NextIndex != -1)
                                    {
                                        long l = block1.NextIndex - rpointer.Index - rpointer.Length;
                                        if (l > max)
                                        {
                                            max = l;
                                        }
                                    }
                                }
                                if (block1.NextIndex == -1)
                                {
                                    max = -1;
                                }
                                else
                                {
                                    block1.MaxSpace = (int)max;
                                }
                            }
                            else
                            {
                                rpointer.Index = offs + bsize + psize * BlockCount;
                                rpointer.Length = size;
                            }
                            //添加写入指针
                            pointers.Add(rpointer);
                            //添加数据块和索引表
                            block1.Count = pointers.Count;
                            while (pointers.Count < BlockCount)
                            {
                                pointers.Add(new Pointer());
                            }
                            //写入数据块
                            fileStream.Position = offs;
                            fileStream.Write(block1.ToBytes());
                            //写入索引表
                            foreach (Pointer item in pointers)
                            {
                                fileStream.Write(item.ToBytes());
                            }
                            rw = false;
                        }
                        else
                        {
                            //如果有下一个区块
                            if (block1.NextIndex != -1)
                            {
                                offs = block1.NextIndex;
                                fileStream.Position = offs;
                                block1 = Block.FormStream(fileStream);
                            }
                            else
                            {
                                //创建区块
                                Block block2 = Block.Default;
                                if (offs == 0)
                                    block2.LastIndex = -1;
                                else
                                    block2.LastIndex = offs;
                                block1.NextIndex = fileStream.Length;
                                //写入第一个区块数据
                                fileStream.Position = offs;
                                fileStream.Write(block1.ToBytes());
                                offs = block1.NextIndex;
                                block1 = block2;
                            }
                        }
                    }
                }
            }
            catch { }
            IsCalloc = false;
            return rpointer;
        }
        /// <summary>
        /// 自动分配指定容量空间，通过集中分配空间极大的提高写入效率，效率低的主要原因是打开和关闭数据流花费时间太久。
        /// </summary>
        /// <param name="sizes"></param>
        /// <returns></returns>
        public Pointer[] Callocs(params long[] sizes)
        {
            while (IsCalloc) Task.Delay(1).Wait();
            IsCalloc = true;
            int bsize = IToBytes.SizeOf<Block>();
            int psize = IToBytes.SizeOf<Pointer>();
            long offs = 0;
            Pointer[] rpointers = new Pointer[sizes.Length];
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                //读取最后一个区块
                Block block1 = Block.FormStream(fileStream);
                while (block1.NextIndex != -1)
                {
                    fileStream.Position = block1.NextIndex;
                    offs = block1.NextIndex;
                    block1 = Block.FormStream(fileStream);
                }
                //数据写入位置
                long pos = bsize + psize * BlockCount;
                //获取Pointer列表
                List<Pointer> pointers = new List<Pointer>();
                if (block1.Count > 0)
                {
                    for (int i = 0; i < BlockCount; i++)
                    {
                        Pointer p = Pointer.FormStream(fileStream);
                        if (p.Length > 0)
                        {
                            pointers.Add(p);
                        }
                    }
                    pointers.Sort();
                    //获得最后的一个数据的位置
                    pos = pointers[^1].Index + pointers[^1].Length;
                }
                for (int i = 0; i < sizes.Length; i++)
                {
                    if (pointers.Count < BlockCount)
                    {
                        rpointers[i].Index = pos;
                        rpointers[i].Length = sizes[i];
                        pos += sizes[i];
                        pointers.Add(rpointers[i]);
                    }
                    else
                    {
                        block1.Count = pointers.Count;
                        //创建区块
                        Block block2 = Block.Default;
                        if (offs == 0)
                            block2.LastIndex = -1;
                        else
                            block2.LastIndex = offs;
                        block1.NextIndex = pos;
                        //写入区块数据
                        fileStream.Position = offs;
                        fileStream.Write(block1.ToBytes());
                        //写入索引
                        foreach (var item in pointers)
                        {
                            fileStream.Write(item.ToBytes());
                        }
                        offs = block1.NextIndex;
                        block1 = block2;
                        pos += bsize + psize * BlockCount;
                    }
                }
                block1.Count = pointers.Count;
                //写入区块数据
                fileStream.Position = offs;
                fileStream.Write(block1.ToBytes());
                //写入索引
                foreach (var item in pointers)
                {
                    fileStream.Write(item.ToBytes());
                }

                fileStream.Close();
            }
            IsCalloc = false;
            return rpointers;
        }
        /// <summary>
        /// 创建一个新的文件数据流
        /// </summary>
        /// <returns></returns>
        public FileStream GetNewFileStream()=> new FileStream(fpath, fmode, faccess, fShare);

        /// <summary>
        /// 移除数据指针
        /// </summary>
        /// <param name="oldpointer"></param>
        /// <param name="newpointer"></param>
        public void PointerRemove(Pointer pointer)
        {
            while (IsCalloc) Task.Delay(1).Wait();
            IsCalloc = true;
            using ( FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                //查找指针所在区块
                long pos = 0;
                Block block = Block.Default;
                do
                {
                    pos = fileStream.Position;
                    block = Block.FormStream(fileStream);
                    //如果下一个区块位置大于指针位置则说明指针被存储在当前区块
                    if (block.NextIndex > pointer.Position || block.NextIndex == -1)
                    {
                        if (block.Count > 0)
                        {
                            fileStream.Position = pos;
                            block.Count--;
                            fileStream.Write(block.ToBytes());
                        }
                        fileStream.Position = pointer.Position;
                        pointer.Length = 0;
                        fileStream.Write(pointer.ToBytes());
                        fileStream.Close();
                        IsCalloc = false;
                        return;
                    }
                } while (block.NextIndex != -1);
            }
            IsCalloc = false;
        }
        /// <summary>
        /// 移除数据指针
        /// </summary>
        /// <param name="oldpointer"></param>
        /// <param name="newpointer"></param>
        public void PointerRemove(FileStream fileStream, Pointer pointer)
        {
            //查找指针所在区块
            long pos = 0;
            Block block = Block.Default;
            do
            {
                pos = fileStream.Position;
                block = Block.FormStream(fileStream);
                //如果下一个区块位置大于指针位置则说明指针被存储在当前区块
                if (block.NextIndex > pointer.Position || block.NextIndex == -1)
                {
                    if (block.Count > 0)
                    {
                        fileStream.Position = pos;
                        block.Count--;
                        fileStream.Write(block.ToBytes());
                    }
                    fileStream.Position = pointer.Position;
                    pointer.Length = 0;
                    fileStream.Write(pointer.ToBytes());
                    return;
                }
            } while (block.NextIndex != -1);
        }
        /// <summary>
        /// 返回从0开始第num个区块，如果区块数量不足num则返回最后一个区块
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        public Block NextBlock(int num = 0)
        {
            using ( FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                Block block = Block.FormStream(fileStream);
                for (int i = 0; i < num; i++)
                {
                    if (block.NextIndex == -1) break;
                    fileStream.Position = block.NextIndex;
                    block = Block.FormStream(fileStream);
                }
                fileStream.Close();
                return block;
            }
        }
        /// <summary>
        /// 获得指定区块的索引表
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        public Pointer[] GetPointers(Block block)
        {
            using ( FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                if(block.LastIndex != -1)
                {
                    fileStream.Position = block.LastIndex + 16;
                    byte[] buffer = new byte[8];
                    if(fileStream.Read(buffer, 0, 8) == -1)return new Pointer[0];
                    fileStream.Position = BitConverter.ToInt64(buffer);
                }
                fileStream.Position += IToBytes.SizeOf<Block>();
                List<Pointer> pointer = new List<Pointer>();
                for (int i = 0; i < BlockCount; i++)
                {
                    Pointer p = Pointer.FormStream(fileStream);
                    if (p.Length > 0)
                    {
                        pointer.Add(p);
                    }
                }
                return pointer.ToArray() ;
            }
        }
        /// <summary>
        /// 获得全部的索引表
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        public Pointer[] GetPointers()
        {
            List<Pointer> pointer = new List<Pointer>();
            int bSize = IToBytes.SizeOf<Block>(); 
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                Block block = Block.Default;
                do
                {
                    block = Block.FormStream(fileStream);
                    for (int i = 0; i < BlockCount; i++)
                    {
                        Pointer p = Pointer.FormStream(fileStream);
                        if (p.Length > 0)
                        {
                            pointer.Add(p);
                        }
                    }
                } while (block.NextIndex != -1);
            }
            return pointer.ToArray();
        }
        /// <summary>
        /// 判断指向数据的头部
        /// </summary>
        /// <param name="pointer"></param>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public bool StartWith(Pointer pointer, ReadOnlySpan<byte> buffer)
        {
            if(buffer.Length > pointer.Length)return false;
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                fileStream.Position = pointer.Index;
                foreach (byte item in buffer)
                {
                    if(item != fileStream.ReadByte())
                    {
                        return false;
                    }
                }
                fileStream.Close();
            }
            return true;
        }
        /// <summary>
        /// 遍历查找头部数据
        /// </summary>
        /// <returns></returns>
        public Pointer FindStart(ReadOnlySpan<byte> buffer)
        {
            int bsize = IToBytes.SizeOf<Block>();
            int psize = IToBytes.SizeOf<Pointer>();
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                for (int i = 0; i < int.MaxValue; i++)
                {
                    Block block = Block.FormStream(fileStream);
                    for (int j = 0; j < BlockCount; j++)
                    {
                        Pointer p = Pointer.FormStream(fileStream);
                        if (p.Length < buffer.Length) continue;
                        long pos = fileStream.Position;
                        //读取并对比数据
                        fileStream.Position = p.Index;
                        bool r = true;
                        foreach (byte item in buffer)
                        {
                            if (item != fileStream.ReadByte())
                            {
                                r = false;
                                break;
                            }
                        }
                        if (r)
                        {
                            return p;
                        }
                        fileStream.Position = pos;
                    }
                    if (block.NextIndex == -1) break;
                    fileStream.Position = block.NextIndex;

                }
            }
            return new Pointer();
        }
        public Pointer FindStart(ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2)
        {
            int bsize = IToBytes.SizeOf<Block>();
            int psize = IToBytes.SizeOf<Pointer>();
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                for (int i = 0; i < int.MaxValue; i++)
                {
                    Block block = Block.FormStream(fileStream);
                    for (int j = 0; j < BlockCount; j++)
                    {
                        Pointer p = Pointer.FormStream(fileStream);
                        if (p.Length < buffer1.Length + buffer2.Length) continue;
                        long pos = fileStream.Position;
                        //读取并对比数据
                        fileStream.Position = p.Index;
                        bool r = true;
                        foreach (byte item in buffer1)
                        {
                            if (item != fileStream.ReadByte())
                            {
                                r = false;
                                break;
                            }
                        }
                        foreach (byte item in buffer2)
                        {
                            if (item != fileStream.ReadByte())
                            {
                                r = false;
                                break;
                            }
                        }
                        if (r)
                        {
                            return p;
                        }
                        fileStream.Position = pos;
                    }
                    if (block.NextIndex == -1) break;
                    fileStream.Position = block.NextIndex;

                }
            }
            return new Pointer();
        }
        public Pointer FindStart(ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2, ReadOnlySpan<byte> buffer3)
        {
            int bsize = IToBytes.SizeOf<Block>();
            int psize = IToBytes.SizeOf<Pointer>();
            using (FileStream fileStream = new FileStream(fpath, fmode, faccess, fShare))
            {
                for (int i = 0; i < int.MaxValue; i++)
                {
                    Block block = Block.FormStream(fileStream);
                    for (int j = 0; j < BlockCount; j++)
                    {
                        Pointer p = Pointer.FormStream(fileStream);
                        if (p.Length < buffer1.Length + buffer2.Length + buffer3.Length) continue;
                        long pos = fileStream.Position;
                        //读取并对比数据
                        fileStream.Position = p.Index;
                        bool r = true;
                        foreach (byte item in buffer1)
                        {
                            if (item != fileStream.ReadByte())
                            {
                                r = false;
                                break;
                            }
                        }
                        foreach (byte item in buffer2)
                        {
                            if (item != fileStream.ReadByte())
                            {
                                r = false;
                                break;
                            }
                        }
                        foreach (byte item in buffer3)
                        {
                            if (item != fileStream.ReadByte())
                            {
                                r = false;
                                break;
                            }
                        }
                        if (r)
                        {
                            return p;
                        }
                        fileStream.Position = pos;
                    }
                    if (block.NextIndex == -1) break;
                    fileStream.Position = block.NextIndex;

                }
            }
            return new Pointer();
        }


        /// <summary>
        /// 往指定地址内存储数据，if(pointer.Length != buffer.Length)return false;
        /// </summary>
        /// <param name="_fileStream"></param>
        /// <param name="pointer"></param>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer)
        {
            try
            {
                if (pointer.Length != buffer.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer);
            }
            catch
            {
                return false;
            }
            return true;
        }
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2)
        {
            try
            {
                if (pointer.Length != buffer1.Length + buffer2.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer1);
                _fileStream.Write(buffer2);
            }
            catch
            {
                return false;
            }
            return true;
        }
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2, ReadOnlySpan<byte> buffer3)
        {
            try
            {
                if (pointer.Length != buffer1.Length + buffer2.Length + buffer3.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer1);
                _fileStream.Write(buffer2);
                _fileStream.Write(buffer3);
            }
            catch
            {
                return false;
            }
            return true;
        }
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2, ReadOnlySpan<byte> buffer3, ReadOnlySpan<byte> buffer4)
        {
            try
            {
                if (pointer.Length != buffer1.Length + buffer2.Length + buffer3.Length + buffer4.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer1);
                _fileStream.Write(buffer2);
                _fileStream.Write(buffer3);
                _fileStream.Write(buffer4);
            }
            catch
            {
                return false;
            }
            return true;
        }
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2, ReadOnlySpan<byte> buffer3, ReadOnlySpan<byte> buffer4, ReadOnlySpan<byte> buffer5)
        {
            try
            {
                if (pointer.Length != buffer1.Length + buffer2.Length + buffer3.Length + buffer4.Length + buffer5.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer1);
                _fileStream.Write(buffer2);
                _fileStream.Write(buffer3);
                _fileStream.Write(buffer4);
                _fileStream.Write(buffer5);
            }
            catch
            {
                return false;
            }
            return true;
        }
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2, ReadOnlySpan<byte> buffer3, ReadOnlySpan<byte> buffer4, ReadOnlySpan<byte> buffer5, ReadOnlySpan<byte> buffer6)
        {
            try
            {
                if (pointer.Length != buffer1.Length + buffer2.Length + buffer3.Length + buffer4.Length + buffer5.Length + buffer6.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer1);
                _fileStream.Write(buffer2);
                _fileStream.Write(buffer3);
                _fileStream.Write(buffer4);
                _fileStream.Write(buffer5);
                _fileStream.Write(buffer6);
            }
            catch
            {
                return false;
            }
            return true;
        }
        public static bool Write(FileStream _fileStream, Pointer pointer, ReadOnlySpan<byte> buffer1, ReadOnlySpan<byte> buffer2, ReadOnlySpan<byte> buffer3, ReadOnlySpan<byte> buffer4, ReadOnlySpan<byte> buffer5, ReadOnlySpan<byte> buffer6, ReadOnlySpan<byte> buffer7)
        {
            try
            {
                if (pointer.Length != buffer1.Length + buffer2.Length + buffer3.Length + buffer4.Length + buffer5.Length + buffer6.Length + buffer7.Length) return false;
                _fileStream.Position = pointer.Index;
                _fileStream.Write(buffer1);
                _fileStream.Write(buffer2);
                _fileStream.Write(buffer3);
                _fileStream.Write(buffer4);
                _fileStream.Write(buffer5);
                _fileStream.Write(buffer6);
                _fileStream.Write(buffer7);
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 往指针位置写入数据
        /// </summary>
        /// <param name="pointer"></param>
        /// <returns></returns>
        public static byte[] Read(FileStream _fileStream, Pointer pointer)
        {
            if (_fileStream.Length < pointer.Index) return new byte[0];
            _fileStream.Position = pointer.Index;
            byte[] buffer = new byte[pointer.Length];
            _fileStream.Read(buffer, 0, buffer.Length);
            return buffer;
        }
        /// <summary>
        /// 往指针位置写入数据
        /// </summary>
        /// <param name="pointer"></param>
        /// <returns></returns>
        public static byte[] Read(FileStream _fileStream, Pointer pointer, int offs)
        {
            if (_fileStream.Length < pointer.Index + pointer.Length)
            {
                return new byte[0];
            }
            if (pointer.Length - offs <= 0)
            {
                return new byte[0];
            }
            _fileStream.Position = pointer.Index + offs;
            byte[] buffer = new byte[pointer.Length - offs];
            _fileStream.Read(buffer, 0, buffer.Length);
            return buffer;
        }
    }
    /// <summary>
    /// 基于Actuator的数据存储表
    /// </summary>
    public class DataList
    {
        public static System.Text.Encoding Encoding = System.Text.Encoding.UTF8;
        /// <summary>
        /// 在写入时，是否检测key已存在，如果检查，则会覆盖原数据,由于读取速度受到数据量影响，当数据量大时，该参数会对写入速度造成巨大影响。
        /// </summary>
        public static bool AllowRepeat { get; set; }
        public static bool Write(string path, string key, byte[] data)
        {
            bool result = false;
            Actuator actuator = new Actuator(path, FileMode.OpenOrCreate);
            byte[] buffer = Encoding.GetBytes(key);
            byte[] len = BitConverter.GetBytes(buffer.Length);
            long size = buffer.Length + 4 + data.Length;
            if (!AllowRepeat)
            {
                using (var fs = actuator.GetNewFileStream())
                {
                    var p = actuator.Calloc(fs, size);
                    result = Actuator.Write(fs, p, len, buffer, data);
                    fs.Close();
                    return result;
                }
            }
            Pointer pointer = actuator.FindStart(len, buffer);
            //如果存在key并且key的容量大于存储数据容量则直接存入
            if (pointer.Length > size)
            {
                using (var fs = actuator.GetNewFileStream())
                {
                    pointer.Length = size;
                    fs.Position = pointer.Position;
                    fs.Write(pointer.ToBytes());
                    result = Actuator.Write(fs, pointer, len, buffer, data);
                    fs.Close();
                }
            }
            else
            {
                using (var fs = actuator.GetNewFileStream())
                {
                    if (pointer.Length > 0)
                        actuator.PointerRemove(fs, pointer);
                    var p = actuator.Calloc(fs,size);
                    result = Actuator.Write(fs, p, len, buffer, data);
                    fs.Close();
                }
            }

            //if (!result) Console.WriteLine("写入失败");
            return result;
          }
        public static bool WriteText(string path, string key, string text)
        {
            return Write(path, key, Encoding.GetBytes(text));
        }
        /// <summary>
        /// 如果key不存在返回null
        /// </summary>
        /// <param name="path"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static byte[]? Read(string path, string key)
        {
            Actuator actuator = new Actuator(path, FileMode.Open);
            byte[] buffer = Encoding.GetBytes(key);
            byte[] len = BitConverter.GetBytes(buffer.Length);
            Pointer pointer = actuator.FindStart(len, buffer);
            if (pointer.Length == 0) return null;
            return actuator.Read(pointer, buffer.Length + 4);
        }
        public static string? ReadText(string path, string key)
        {
            byte[] buffer = Read(path, key);
            if (buffer == null) return null;
            return Encoding.GetString(buffer);
        }
        public static int GetCount(string path)
        {
            Actuator actuator = new Actuator(path, FileMode.OpenOrCreate);
            int count = 0;
            int n = 0;
            Block block = actuator.NextBlock(n++);
            count += block.Count;
            while (block.NextIndex > -1)
            {
                block = actuator.NextBlock(n++);
                count += block.Count;
            }
            //Console.WriteLine(actuator.NextBlock());
            return count;
        }
        public static void Remove(string path, string key)
        {
            Actuator actuator = new Actuator(path, FileMode.Open);
            byte[] buffer = Encoding.GetBytes(key);
            byte[] len = BitConverter.GetBytes(buffer.Length);
            Pointer pointer = actuator.FindStart(len, buffer);
            actuator.PointerRemove(pointer);
        }
        public static bool Contains(string path, string key)
        {
            Actuator actuator = new Actuator(path, FileMode.Open);
            byte[] buffer = Encoding.GetBytes(key);
            byte[] len = BitConverter.GetBytes(buffer.Length);
            if (actuator.FindStart(len, buffer).Length > 0)return true;
            return false;
        }
        public static void Clear(string path)
        {
            new Actuator(path, FileMode.Create);
        }
        public static string[] GetKeys(string path)
        {
            List<string> keys = new List<string>();
            List<Pointer> pointers = new List<Pointer>();
            Actuator actuator = new Actuator(path, FileMode.OpenOrCreate);
            int bSize = IToBytes.SizeOf<Block>();
            using (FileStream fileStream = actuator.GetNewFileStream())
            {
                Block block = Block.Default;
                do
                {
                    block = Block.FormStream(fileStream);
                    for (int i = 0; i < actuator.BlockCount; i++)
                    {
                        Pointer p = Pointer.FormStream(fileStream);
                        if (p.Length > 0)
                        {
                            pointers.Add(p);
                        }
                    }
                } while (block.NextIndex != -1);
                byte[] len = new byte[4];
                foreach (Pointer p in pointers)
                {
                    fileStream.Position = p.Index;
                    fileStream.Read(len);
                    byte[] data = new byte[BitConverter.ToInt32(len)];
                    fileStream.Read(data);
                    keys.Add(Encoding.GetString(data, 0, data.Length));
                }
                fileStream.Close();
            }
            return keys.ToArray();
        }
    }
    public struct Block: IToBytes
    {
        public static Block Default = new Block();
        /// <summary>
        /// 这个区块能单独使用的最大的空间，-1为无限
        /// </summary>
        public int MaxSpace = -1;
        /// <summary>
        /// 区块中索引的数量
        /// </summary>
        public int Count = 0;
        public long LastIndex = -1;
        public long NextIndex = -1;
        public byte[] ToBytes()
        {
            byte[] buffer = new byte[24];
            BitConverter.GetBytes(MaxSpace).CopyTo(buffer, 0);
            BitConverter.GetBytes(Count).CopyTo(buffer, 4);
            BitConverter.GetBytes(LastIndex).CopyTo(buffer, 8);
            BitConverter.GetBytes(NextIndex).CopyTo(buffer, 16);
            return buffer;
        }
        public override string ToString()
        {
            return $"MaxSpace:{MaxSpace} Count:{Count} LastIndex:{LastIndex} NextIndex:{NextIndex}";
        }
        public static Block FormStream(Stream stream)
        {
            byte[] bt1 = new byte[24];
            stream.Read(bt1, 0, bt1.Length);
            return new Block{
                MaxSpace = BitConverter.ToInt32(bt1, 0),
                Count = BitConverter.ToInt32(bt1, 4),
                LastIndex = BitConverter.ToInt64(bt1, 8),
                NextIndex = BitConverter.ToInt64(bt1, 16),
            };
        }
        public static Block FormBytes(byte[] bytes)
        {
            return new Block
            {
                MaxSpace = BitConverter.ToInt32(bytes, 0),
                Count = BitConverter.ToInt32(bytes, 4),
                LastIndex = BitConverter.ToInt64(bytes, 8),
                NextIndex = BitConverter.ToInt64(bytes, 16),
            };
        }

    }

    public struct Pointer : IComparable<Pointer>, IToBytes
    {
        public static Pointer Default => new Pointer();
        public Pointer(byte[] bytes)
        {
            Index = BitConverter.ToInt64(bytes, 0);
            Length = BitConverter.ToInt64(bytes, 4);
        }
        public Pointer(long index, long length)
        {
            Index = index;
            Length = length;
        }
        public long Index = 0;
        public long Length = 0;
        //当前指针的存储位置，该位置不会写入文件,只有FormStream获取的Pointer具备该值
        public long Position = 0;
        //public byte[] ToBytes() => IToBytes.ToBytes(this);
        public override string ToString()
        {
            return $"Index:{Index} Length{Length}";
        }
        public int CompareTo(Pointer other)
        {
            return Index.CompareTo(other.Index);
        }

        public byte[] ToBytes()
        {
            byte[] buffer = new byte[16];
            BitConverter.GetBytes(Index).CopyTo(buffer, 0);
            BitConverter.GetBytes(Length).CopyTo(buffer, 8);
            return buffer;
        }
        public Pointer Copy()
        {
            return new Pointer() { Index = this.Index,Length = this.Length,Position = this.Position };
        }
        public static Pointer FormStream(Stream stream)
        {
            byte[] bt1 = new byte[16];
            long pos = stream.Position;
            stream.Read(bt1, 0, bt1.Length);
            return new Pointer
            {
                Position = pos,
                Index = BitConverter.ToInt64(bt1, 0),
                Length = BitConverter.ToInt64(bt1, 8),
            };
        }
        
    }

    interface IToBytes
    {
        byte[] ToBytes();
        static int SizeOf<T>() where T : unmanaged
        {
            unsafe { return sizeof(T); }
        }
        //static T FromBytes<T>(Span<byte> data) where T : unmanaged
        //{
        //    unsafe
        //    {
        //        T r;
        //        byte* p = (byte*)&r;
        //        int size = sizeof(T);
        //        for (int i = 0; i < size; i++)
        //        {
        //            *p = data[i];
        //            p++;
        //        }
        //        return r;
        //    }
        //}
        //static Span<byte> ToBytes<T>(T p)where T : unmanaged
        //{
        //    unsafe
        //    {
        //        return new Span<byte>(&p, sizeof(T));
        //    }
        //}
        //static Span<byte> ToSpanBytes<T>(T[] datas) where T : unmanaged
        //{
        //    unsafe
        //    {
        //        fixed (T* p = &datas[0])
        //        {
        //            return new Span<byte>(p, datas.Length * sizeof(T));
        //        }
        //    }
        //}
    }
}
