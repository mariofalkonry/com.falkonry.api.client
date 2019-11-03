using System;
using System;
using System.IO;
using System.Linq;

namespace DataSanitizer
{
    class Program
    {
        static string sourceRootPath = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\ToBeProcessed\\TorbenSpirit\\2018";
        static string destRootPath = "C:\\Users\\m2bre\\Documents\\Projects\\API\\TestData";
        static string fileNameRoot = "WideFormatSmall";
        static Tuple<string, string>[] toReplace = new Tuple<string, string>[]
        {
            new Tuple<string, string>("TorbenSpirit","Equipment1"),
            new Tuple<string, string>("C0/M24/TemperatureBearing2/SingleArithAverageValue","Signal1"),
            new Tuple<string, string>("C5/M111/TemperatureSuctionStage3/TemperatureSuctionSide","Signal2"),
            new Tuple<string, string>("C0/M28/TemperatureBearing6/SingleArithAverageValue","Signal3"),
            new Tuple<string, string>("C0/M808/PressureSuctionStage1/SuctionPressure","Signal4"),
            new Tuple<string, string>("C4/M103/PressureDischargeStage2/DischargePressure","Signal5"),
            new Tuple<string, string>("C4/M11/VibrationChs4St2/SingleRmsValue","Signal6"),
            new Tuple<string, string>("C6/M22/PressureCyl6St1Ce/BreakThroughSuctionPressure","Signal7"),
            new Tuple<string, string>("C3/M7/VibrationChs3St4/5/SingleRmsValue","Signal8"),
            new Tuple<string, string>("C3/M8/VibrationCyl3St4/5/SingleRmsValue","Signal9"),
            new Tuple<string, string>("C3/M106/PressureDischargeStage5/DischargePressure","Signal10"),
            new Tuple<string, string>("C0/M26/TemperatureBearing4/SingleArithAverageValue","Signal11"),
            new Tuple<string, string>("C3/M113/TemperatureSuctionStage4/TemperatureSuctionSide","Signal12"),
            new Tuple<string, string>("C4/M14/PressureCyl4St2Ce/BreakThroughSuctionPressure","Signal13"),
            new Tuple<string, string>("C3/M9/PressureCyl3St4He/BreakThroughDischargePressure","Signal14"),
            new Tuple<string, string>("C5/M112/TemperatureDischargeStage3/TemperatureDischargeSide","Signal15"),
            new Tuple<string, string>("C0/M23/TemperatureBearing1/SingleArithAverageValue","Signal16"),
            new Tuple<string, string>("C0/M29/TemperatureBearing7/SingleArithAverageValue","Signal17"),
            new Tuple<string, string>("C4/M14/PressureCyl4St2Ce/BreakThroughDischargePressure","Signal18"),
            new Tuple<string, string>("C5/M18/PressureCyl5St3Ce/BreakThroughDischargePressure","Signal19"),
            new Tuple<string, string>("C3/M115/TemperatureSuctionStage5/TemperatureSuctionSide","Signal20"),
            new Tuple<string, string>("C3/M9/PressureCyl3St4He/BreakThroughSuctionPressure","Signal21"),
            new Tuple<string, string>("C4/M12/VibrationCyl4St2/SingleRmsValue","Signal22"),
            new Tuple<string, string>("C3/M114/TemperatureDischargeStage4/TemperatureDischargeSide","Signal23"),
            new Tuple<string, string>("C2/M6/VibrationChs2/SingleRmsValue","Signal24"),
            new Tuple<string, string>("C1/M4/PressureCyl1St1He/BreakThroughSuctionPressure","Signal25"),
            new Tuple<string, string>("C1/M3/VibrationCyl1St1/SingleRmsValue","Signal26"),
            new Tuple<string, string>("C0/M117/TemperatureBearingMotorNde/MotorBearingTemperature","Signal27"),
            new Tuple<string, string>("C0/M107/TemperatureSuctionStage1/TemperatureSuctionSide","Signal28"),
            new Tuple<string, string>("C4/M110/TemperatureDischargeStage2/TemperatureDischargeSide","Signal29"),
            new Tuple<string, string>("C6/M21/PressureCyl6St1He/BreakThroughDischargePressure","Signal30"),
            new Tuple<string, string>("C6/M20/VibrationCyl6St1/SingleRmsValue","Signal31"),
            new Tuple<string, string>("C6/M19/VibrationChs6St1/SingleRmsValue","Signal32"),
            new Tuple<string, string>("C0/M108/TemperatureDischargeStage1/TemperatureDischargeSide","Signal33"),
            new Tuple<string, string>("C3/M10/PressureCyl3St5Ce/BreakThroughDischargePressure","Signal34"),
            new Tuple<string, string>("C0/M118/TemperatureBearingMotorDe/MotorBearingTemperature","Signal35"),
            new Tuple<string, string>("C0/M27/TemperatureBearing5/SingleArithAverageValue","Signal36"),
            new Tuple<string, string>("C4/M13/PressureCyl4St2He/BreakThroughSuctionPressure","Signal37"),
            new Tuple<string, string>("C5/M16/VibrationCyl5St3/SingleRmsValue","Signal38"),
            new Tuple<string, string>("C5/M18/PressureCyl5St3Ce/BreakThroughSuctionPressure","Signal39"),
            new Tuple<string, string>("C5/M17/PressureCyl5St3He/BreakThroughSuctionPressure","Signal40"),
            new Tuple<string, string>("C3/M10/PressureCyl3St5Ce/BreakThroughSuctionPressure","Signal41"),
            new Tuple<string, string>("C0/M102/PressureDischargeStage1/DischargePressure","Signal42"),
            new Tuple<string, string>("C5/M17/PressureCyl5St3He/BreakThroughDischargePressure","Signal43"),
            new Tuple<string, string>("C6/M22/PressureCyl6St1Ce/BreakThroughDischargePressure","Signal44"),
            new Tuple<string, string>("C3/M105/PressureDischargeStage4/DischargePressure","Signal45"),
            new Tuple<string, string>("C4/M109/TemperatureSuctionStage2/TemperatureSuctionSide","Signal46"),
            new Tuple<string, string>("C4/M13/PressureCyl4St2He/BreakThroughDischargePressure","Signal47"),
            new Tuple<string, string>("C5/M104/PressureDischargeStage3/DischargePressure","Signal48"),
            new Tuple<string, string>("C1/M2/VibrationChs1St1/SingleRmsValue","Signal49"),
            new Tuple<string, string>("C0/M25/TemperatureBearing3/SingleArithAverageValue","Signal50"),
            new Tuple<string, string>("C4/M116/TemperatureDischargeStage5/TemperatureDischargeSide","Signal51"),
            new Tuple<string, string>("C1/M5/PressureCyl1St1Ce/BreakThroughSuctionPressure","Signal52"),
            new Tuple<string, string>("C6/M21/PressureCyl6St1He/BreakThroughSuctionPressure","Signal53"),
            new Tuple<string, string>("C1/M4/PressureCyl1St1He/BreakThroughDischargePressure","Signal54"),
            new Tuple<string, string>("C5/M15/VibrationChs5St3/SingleRmsValue","Signal55"),
        };

        static void Main(string[] args)
        {
            var files = from file in Directory.EnumerateFiles(sourceRootPath, "*.csv", SearchOption.AllDirectories) select file;
            var i = 1;
            foreach (var file in files)
            {
                var linesIn = File.ReadLines(file);
                var destPath = Path.Combine(destRootPath, $"{fileNameRoot+i++}.csv");
                using (var dest = new StreamWriter(File.Open(destPath, FileMode.Create)))
                {
                    foreach (var line in linesIn)
                    {
                        var newline = new string(line);
                        foreach (var torep in toReplace)
                        {
                            newline=newline.Replace(torep.Item1, torep.Item2);
                        }
                        dest.WriteLine(newline);
                    }
                }
                Console.WriteLine($"Sanitized file {file} to {destPath}");
            }
        }
    }
}
