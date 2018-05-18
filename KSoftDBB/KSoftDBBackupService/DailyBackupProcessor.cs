using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.IO;

namespace KSoftDBBackupService
{

    class DailyBackupProcessor
    {

        public static void CreateFullBackup(int month)
        {
            // read connectionstring from config file
            var connectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;

            // read backup folder from config file ("C:/temp/")
            var backupFolder = ConfigurationManager.AppSettings["BackupFolder"] + month.ToString() +"/";

            var sqlConStrBuilder = new SqlConnectionStringBuilder(connectionString);

            System.IO.Directory.CreateDirectory(backupFolder);

            // set backupfilename (you will get something like: "C:/temp/MyDatabase-2013-12-07.bak")
            var backupFileName = String.Format("{0}{1}-{2}.bak",
                backupFolder, sqlConStrBuilder.InitialCatalog,
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss").Replace(':', '-'));
            try
            {

                using (MySqlConnection conn = new MySqlConnection(connectionString))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        using (MySqlBackup mb = new MySqlBackup(cmd))
                        {
                            cmd.Connection = conn;
                            conn.Open();
                            mb.ExportInfo.MaxSqlLength = 0;
                            //mb.ExportInfo.AddCreateDatabase = true;
                            //mb.ExportInfo.EnableEncryption = true;
                            //mb.ExportInfo.EncryptionPassword = "ksoft123";
                            mb.ExportToFile(backupFileName);
                            conn.Close();
                            
                        }
                    }
                }

            }
            catch( Exception ex)
            {
                Logger.log(ex.Message);
            }

            
        }
        public static void InstantFullBackup()
        {
            // read connectionstring from config file
            var connectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;

            // read backup folder from config file ("C:/temp/")
            var backupFolder = ConfigurationManager.AppSettings["BackupFolder"] +  "instant backup/";

            var sqlConStrBuilder = new SqlConnectionStringBuilder(connectionString);

            System.IO.Directory.CreateDirectory(backupFolder);

            // set backupfilename (you will get something like: "C:/temp/MyDatabase-2013-12-07.bak")
            var backupFileName = String.Format("{0}{1}-{2}.bak",
                backupFolder, sqlConStrBuilder.InitialCatalog,
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss").Replace(':', '-'));
            try
            {

                using (MySqlConnection conn = new MySqlConnection(connectionString))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        using (MySqlBackup mb = new MySqlBackup(cmd))
                        {
                            cmd.Connection = conn;
                            conn.Open();
                            mb.ExportInfo.MaxSqlLength = 0;
                            //mb.ExportInfo.AddCreateDatabase = true;
                            //mb.ExportInfo.EnableEncryption = true;
                            //mb.ExportInfo.EncryptionPassword = "ksoft123";
                            mb.ExportToFile(backupFileName);
                            conn.Close();

                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.log(ex.Message);
            }


        }
        public static void CreateIncrementalBackup(int month)
        {
            // read connectionstring from config file
            var connectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;

            // read backup folder from config file ("C:/temp/")
            var backupFolder = ConfigurationManager.AppSettings["BackupFolder"] + month.ToString() + "/";

            var addCommentInIncreamentalBackup = ConfigurationManager.AppSettings["AddCommentsInIncreamentalBackup"];

            var sqlConStrBuilder = new SqlConnectionStringBuilder(connectionString);

            System.IO.Directory.CreateDirectory(backupFolder);

            // set backupfilename (you will get something like: "C:/temp/MyDatabase-2013-12-07.bak")
            var backupFileName = String.Format("{0}{1}-{2}.bak",
                backupFolder, sqlConStrBuilder.InitialCatalog,
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss").Replace(':', '-'));
            try
            {
                using (MySqlConnection conn = new MySqlConnection(connectionString))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        using (MySqlBackup mb = new MySqlBackup(cmd))
                        {
                            DirectoryInfo d = new DirectoryInfo(backupFolder);
                            FileInfo[] Files = d.GetFiles("*.bak"); //Getting Text files
                            bool isFullBckexists = false;
                            String fullBckFile = "";
                            foreach (FileInfo file in Files)
                            {
                                if (file.Name.Length == backupFileName.Length - backupFolder.Length)
                                {
                                    isFullBckexists = true;
                                    fullBckFile = file.Name;
                                }
                            }
                            cmd.Connection = conn;
                            conn.Open();
                            mb.ExportInfo.MaxSqlLength = 0;
                            //mb.ExportInfo.AddCreateDatabase = true;
                            //mb.ExportInfo.EnableEncryption = true;
                            //mb.ExportInfo.EncryptionPassword = "ksoft123";
                            mb.ExportToFile(backupFileName);
                            conn.Close();

                            if(isFullBckexists)
                            {
                                Logger.log("full back up exists");
                                var file1 = File.ReadAllLines(backupFolder + fullBckFile);
                                var file2 = File.ReadAllLines(backupFileName);

                                List<String> fixedFile1 = new List<string>();
                                List<String> fixedFile2 = new List<string>();

                                String tempLine = String.Empty;
                                foreach (String line in file1)
                                {
                                    if(line.StartsWith("--") || line.StartsWith("/*"))
                                    {
                                        //fixedFile1.Add(line);
                                    }
                                    else if(!line.EndsWith(";"))
                                    {
                                        tempLine += line;
                                    }
                                    else if(line.EndsWith(";"))
                                    {
                                        tempLine += line;
                                        fixedFile1.Add(tempLine);
                                        tempLine = String.Empty;
                                    }

                                }
                                tempLine = String.Empty;
                                foreach (String line in file2)
                                {
                                    if (line.StartsWith("--") || line.StartsWith("/*"))
                                    {
                                        if(addCommentInIncreamentalBackup.Equals("YES"))
                                            fixedFile2.Add(line);
                                    }
                                    else if(line.Equals(String.Empty))
                                    {
                                        if (addCommentInIncreamentalBackup.Equals("YES"))
                                            fixedFile2.Add(line);
                                    }
                                    else if (!line.EndsWith(";"))
                                    {
                                        tempLine += line;
                                    }
                                    else if (line.EndsWith(";"))
                                    {
                                        tempLine += line;
                                        fixedFile2.Add(tempLine);
                                        tempLine = String.Empty;
                                    }
                                    

                                }
                                file1 = fixedFile1.ToArray();
                                file2 = fixedFile2.ToArray();

                                var dupes = Array.FindAll(file2, line =>
                                    Array.Exists(file1, line1 => line1 == line));
                                var noDupes = Array.FindAll(file2, line =>
                                    !Array.Exists(dupes, line2 => line2 == line));

                                string startText = sqlConStrBuilder.InitialCatalog;
                                var start = backupFileName.LastIndexOf(startText) + startText.Length + 1;
                                var end = backupFileName.IndexOf(".");
                                var length = end - start;
                                String secondPart = backupFileName.Substring(start, length);


                                String newFileName = backupFolder+ fullBckFile.Remove(fullBckFile.Length - 4) + "___" + secondPart + ".bak";
                                File.WriteAllLines(newFileName, noDupes); // write back to file1
                                System.IO.File.Delete(backupFileName);
                            }
                            

                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.log(ex.Message);
            }


        }



    }
}
