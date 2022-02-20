using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LargeFile
{
    class Program
    {
        private static readonly object lockObj = new object();
        private static readonly object lockObjWordCount = new object();
        private static string inputFile;
        private static string wordCountEachLineoutputFile = "totalWordCountForEachLine.txt";
        private static string uniqueWordCountOutputFile = "uniqueWordCount.txt";
        private static int numberOfFilesToProcessByEachThread = 3;
        private static int startLineRange = 0;
        private static int endLineRange = 2000000;

        static async Task Main(string[] args)
        {
            /*
             * The program can be run by multiple cpu, by providing the line range to scan in a file.
             * How this program works:
             * 1. Creates multiple threads for a partition of lines.
             * 2. Each thread will 
             *    A:
             *      Calculate the word count of some number of lines and write it to the result file (File 1).
             *    B:
             *      Calcuate the word occurrence from the line in A.And creates a temp file for each word.
             * 3. Merges the temp file created in 2B and creates a single file for (File 2) result.
             * 
             * PRO: less memory usage. Con: High I/O access.
             */

            if (args.Length != 1)
            {
                return;
            }

            inputFile = args[0];

            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            List<Task> tasks = new List<Task>();

            for (int i = startLineRange; i < endLineRange; i++)
            {
                if (i % numberOfFilesToProcessByEachThread == 0)
                {
                    int skip = i;
                    tasks.Add(Task.Run(() => Process(skip, 3)));
                }
            }

            await Task.WhenAll(tasks);

            MergeWordCountFiles();

            watch.Stop();
            Console.WriteLine("Total execution time : " + watch.ElapsedMilliseconds + " ms");
        }

        private static void Process(int skip, int take)
        {
            var lines = File.ReadLines(inputFile).Skip(skip).Take(take);

            int lineNumber = skip + 1;

            Dictionary<string, int> wordCount = new Dictionary<string, int>();

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var words = line.Split(' ');
                var totalWordCountForThisLine = words.Count();

                foreach (var w in words)
                {
                    if (wordCount.ContainsKey(w))
                    {
                        wordCount[w] = wordCount[w] + 1;
                    }
                    else
                    {
                        wordCount[w] = 1;
                    }
                }

                lock (lockObj)
                {
                    using (var w = File.AppendText(wordCountEachLineoutputFile))
                    {
                        w.WriteLine(lineNumber++ + " " + totalWordCountForThisLine);
                    }
                }
            }

            WordCount(wordCount);
        }

        private static void WordCount(Dictionary<string,int> wordCount)
        {
            lock(lockObjWordCount)
            {
                foreach (var kv in wordCount)
                {
                    int value = kv.Value;

                    if (File.Exists($"uniquewordcount-{kv.Key}.txt"))
                    {
                        var currentWordCount = File.ReadLines($"uniquewordcount-{kv.Key}.txt").Take(1).First();

                        string[] cw = currentWordCount.Split();

                        value += int.Parse(cw[1]); 
                    }

                    using (var w = File.CreateText($"uniquewordcount-{kv.Key}.txt"))
                    {
                        w.WriteLine(kv.Key + " " + value);
                    }
                }
            }
            
        }

        private static void MergeWordCountFiles()
        {
            var files = Directory.GetFiles(Directory.GetCurrentDirectory(), "uniquewordcount-*");

            using (var w = File.CreateText(uniqueWordCountOutputFile))
            {
                foreach (var f in files)
                {
                    var currentWordCount = File.ReadLines(f).Take(1).First();
                    w.WriteLine(currentWordCount);
                    File.Delete(f);
                }
            }

        }
    }
}
