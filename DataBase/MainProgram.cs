using System.Collections;
namespace DataBase
{
    class MainProgram
    {
        static void Log(params byte[] bs)
        {
            foreach (byte b in bs)
            {
                Console.Write(b + " ");
            }
            Console.Write("\n");
        }
        /// <summary>
        /// 并发测试
        /// </summary>
        static void Test1()
        {
            Console.WriteLine("r <num> => read data; w <data> => write data; c => get data list count;");

            List<Pointer> pointers = new List<Pointer>();
            new Actuator("abc.d", FileMode.Create);
            Task.Run(() =>
            {
                while (true)
                {
                    lock (pointers)
                    {
                        pointers.Add(new Actuator("abc.d").Write(System.Text.Encoding.UTF8.GetBytes("hello " + pointers.Count)));
                    }
                }
            });
            Console.WriteLine("start write");
            while (true)
            {
                string[] ts = Console.ReadLine().Trim().Split(' ');
                string t = ts[0].Trim();
                if(t == "start")
                {
                    Task.Run(() =>
                    {
                        while (true)
                        {
                            lock (pointers)
                            {
                                pointers.Add(new Actuator("abc.d").Write(System.Text.Encoding.UTF8.GetBytes("world " + pointers.Count)));
                            }
                        }
                    });
                }
                if(t == "c")
                {
                    Console.WriteLine("count:" + pointers.Count);
                }
                if (ts.Length < 2) continue;
                if (t == "r")
                {
                    int c = int.Parse(ts[1]);
                    Console.WriteLine(System.Text.Encoding.UTF8.GetString(new Actuator("abc.d").Read(pointers[c])));
                }
                if (t == "w")
                {
                    lock (pointers)
                    {
                        var a = new Actuator("abc.d").Write(System.Text.Encoding.UTF8.GetBytes(ts[1]));
                        int c = pointers.Count;
                        pointers.Add(a);
                        Console.WriteLine("num:" + c);
                    }
                }
            }
        }
        static void Test2(int count = 1)
        {
            Console.WriteLine("start test");

            List<Pointer> pointers = new List<Pointer>();
            new Actuator("abc.d", FileMode.Create);
            for (int i = 0; i < count; i++)
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        pointers.Add(new Actuator("abc.d").Write(System.Text.Encoding.UTF8.GetBytes("hello " + pointers.Count)));
                    }
                });
            }

            System.Timers.Timer time1 = new System.Timers.Timer(1000);
            int num = pointers.Count;
            time1.Elapsed += (object? sender, System.Timers.ElapsedEventArgs e) => {
                int top = Console.CursorTop;
                int left = Console.CursorLeft;
                Console.CursorVisible = false;
                Console.CursorTop = 0;
                Console.CursorLeft = 0;
                Console.WriteLine($"{pointers.Count - num} n/s    ");
                Console.CursorTop = top;
                Console.CursorLeft = left;
                Console.CursorVisible = true;
                num = pointers.Count;
            };
            time1.Start();
            Console.WriteLine("r <num> => read data; w <data> => write data; c => get data list count;");
            while (true)
            {
                string[] ts = Console.ReadLine().Trim().Split(' ');
                string t = ts[0].Trim();
                if (t == "start")
                {
                    Task.Run(() =>
                    {
                        while (true)
                        {
                            pointers.Add(new Actuator("abc.d").Write(System.Text.Encoding.UTF8.GetBytes("world " + pointers.Count)));
                        }
                    });
                }
                if (t == "c")
                {
                    Console.WriteLine("count:" + pointers.Count);
                }
                if (ts.Length < 2) continue;
                if (t == "r")
                {
                    int c = int.Parse(ts[1]);
                    Console.WriteLine(System.Text.Encoding.UTF8.GetString(new Actuator("abc.d").Read(pointers[c])));
                }
                if (t == "w")
                {
                    lock (pointers)
                    {
                        var a = new Actuator("abc.d").Write(System.Text.Encoding.UTF8.GetBytes(ts[1]));
                        int c = pointers.Count;
                        pointers.Add(a);
                        Console.WriteLine("num:" + c);
                    }
                }
            }

        }
        static void Text3(string path)
        {
            //File.Delete(path);
            Console.WriteLine("r <key> => read data;\nw <key,data> => write data;\nc => get data list count;\nre <key> => remove data;\ncontains <key> => return true or false;\nclear => clear datas;\nkeys => return keys");
            while (true)
            {
                try
                {
                    string[] ts = Console.ReadLine().Trim().Split(' ');
                    string t = ts[0].Trim();
                    if (t == "c")
                    {
                        Console.WriteLine("count:" + DataList.GetCount(path));
                    }
                    if (t == "clear")
                    {
                        DataList.Clear(path);
                    }
                    if (t == "keys")
                    {
                        var ks = DataList.GetKeys(path);
                        foreach (var k in ks)
                        {
                            Console.WriteLine(k);
                        }
                    }
                    if (ts.Length < 2) continue;
                    if (t == "r")
                    {
                        Console.WriteLine(DataList.ReadText(path, ts[1]));
                    }
                    if (t == "re")
                    {
                        DataList.Remove(path, ts[1]);
                    }
                    if (t == "contains")
                    {
                        Console.WriteLine(DataList.Contains(path, ts[1]));
                    }
                    if (ts.Length < 3) continue;
                    if (t == "w")
                    {
                        DataList.WriteText(path, ts[1], ts[2]);
                    }

                }
                catch { Console.WriteLine("错误"); }
            }
        }
        
        static void Text4(string path)
        {
            File.Delete(path);
            Console.WriteLine("start");
            System.Timers.Timer time1 = new System.Timers.Timer(1000);
            int c = 0;
            int num = 0;
            time1.Elapsed += (object? sender, System.Timers.ElapsedEventArgs e) => {
                int top = Console.CursorTop;
                int left = Console.CursorLeft;
                Console.CursorVisible = false;
                Console.CursorTop = 0;
                Console.CursorLeft = 0;
                Console.WriteLine($"{c - num} n/s    ");
                Console.CursorTop = top;
                Console.CursorLeft = left;
                Console.CursorVisible = true;
                num = c;
            };
            time1.Start();
            Task.Run(() =>{
                for (int i = 0; i < int.MaxValue; i++)
                {
                    try
                    {
                        DataList.WriteText(path, i.ToString(), "wow" + i.ToString());
                        c++;
                    }
                    catch
                    {
                    }
    
                }
                Console.WriteLine("end");
            });
            Text3(path);
        }

        static void Main(string[] args)
        {
            //args = new string[] { "abc.d" };
            if (args.Length == 0) return;
            string path = Path.GetFullPath(args[0]);

            DataList.AllowRepeat = true;
            if (args.Length == 1)
            {
                Console.WriteLine("number of key is " + DataList.GetCount(path));
                Console.WriteLine("get<key>   set<key,value>   remove<key>   clear   keys");

            }
            if (args.Length >= 2)
            {
                if (args[1] == "clear")
                {
                    DataList.Clear(path);
                    return;
                }
                if (args[1] == "keys")
                {
                    Console.WriteLine("keys:");
                    int i = 1;
                    foreach (var d in DataList.GetKeys(path))
                    {
                        Console.WriteLine($"{i++}. {d}");
                    }
                    return;
                }
            }
            if (args.Length >= 3)
            {
                string key = args[2].Trim();
                if (args[1] == "remove")
                {
                    if (key != "")
                        DataList.Remove(path, key);
                    return;
                }
                if (args[1] == "get")
                {
                    string r = DataList.ReadText(path, key);
                    if (r != null)
                        Console.WriteLine(r);
                    else
                        Console.WriteLine($"null");
                    return;
                }
            }
            if(args.Length >= 4)
            {
                string key = args[2].Trim();
                string value = args[3].Trim();
                if (args[1] == "set")
                {
                    if (DataList.WriteText(path, key, value))
                        Console.WriteLine("set ok");
                    return;
                }
            }

            //if (args.Length >= 2)
            //{
            //    if (args[0] == "get")
            //    {
            //        if (ds.Contains(args[1]))
            //            Console.WriteLine(System.Text.UTF8Encoding.UTF8.GetString(Actuator.Get(args[1])));
            //        else
            //            Console.WriteLine("there is no key for " + args[1]);
            //        return;
            //    }
            //    if (args[0] == "getn")
            //    {
            //        int i = 0;
            //        if (int.TryParse(args[1], out i))
            //        {
            //            i--;
            //            if (i < 0 || i >= ds.Count)
            //            {
            //                Console.WriteLine("there is no key number for " + args[1]);
            //                return;
            //            }
            //            Console.WriteLine(System.Text.UTF8Encoding.UTF8.GetString(Actuator.GetAt(i)));
            //        }
            //        else
            //            Console.WriteLine("there is no key number for " + args[1]);
            //        return;
            //    }
            //    if (args[0] == "set")
            //    {
            //        byte[] data = null;
            //        if (args.Length >= 3)
            //            data = System.Text.UTF8Encoding.UTF8.GetBytes(args[2]);
            //        Actuator.Set(args[1], data);
            //        if (data != null)
            //            Console.WriteLine("OK");
            //        else
            //            Console.WriteLine("Delete " + args[1]);
            //    }
            //}
        }
    }


}