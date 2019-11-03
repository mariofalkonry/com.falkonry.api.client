using System;
using System.IO;
using System.Linq;

namespace FileCopy
{
    class Program
     { 
        static string sourceRootPath = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\ToBeProcessed\\TorbenSpirit";
        static string destRootPath = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\CSVFiles";
        static string[] vessels = new string[] { "CreoleSpirit", "OAKSpirit", "TorbenSpirit" };
        static void Main(string[] args)
        {
            var files = from file in Directory.EnumerateFiles(sourceRootPath, "*.csv", SearchOption.AllDirectories) select file;
            foreach (var file in files)
            {
                var vessel = vessels.Where(v => file.Contains(v)).First();
                var fileName = file.Substring(file.LastIndexOf("\\"));
                var destPath = $"{destRootPath}\\{vessel}\\{fileName}";
                File.Copy(file, destPath, true);
                Console.WriteLine($"Copied file {file} to {destPath}");
            }
        }
    }
}
