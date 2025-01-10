using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Xml;
using System.Threading;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Text;
using System.Data.Common;

namespace ImportExternalCoupon
{
    class Program
    {
        static bool batchManagerStarted = false;
        static int cvfilecount = 1;
        static string FileName = "";

        static void Main(string[] args)
        {
            int documentsPerXml = Convert.ToInt32(ConfigurationManager.AppSettings["DocumentsPerXml"]);
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory; // Get the base directory of the application
            string csvFolderPath = Path.Combine(baseDirectory, ConfigurationManager.AppSettings["PathForImportExternalCoupon"]);
            string pathForXmlFile = Path.Combine(baseDirectory, ConfigurationManager.AppSettings["PathForImportExternalCouponXMLFile"]);
            string archiveFolderPath = Path.Combine(baseDirectory, ConfigurationManager.AppSettings["PathForImportExternalCouponFileArchive"]);
            string logFolderPath = Path.Combine(baseDirectory, ConfigurationManager.AppSettings["LogFolderPath"]);

            EnsureFolderExists(archiveFolderPath);
            EnsureFolderExists(logFolderPath);
            Console.WriteLine("XML generation started.");
            string[] csvFiles = Directory.GetFiles(csvFolderPath, "*.csv");
            bool xmlGenerated = false;

            foreach (var csvFilePath in csvFiles)
            {
                if (!IsFileNameLegal(csvFilePath, logFolderPath))
                {
                    LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "NCR Illegal file name detected", logFolderPath);
                    continue; // Move to the next CSV file
                }
                GenerateXmlDocuments(documentsPerXml, csvFilePath, pathForXmlFile, archiveFolderPath, logFolderPath);
                xmlGenerated = true;
                if (!xmlGenerated)
                {
                    Console.WriteLine("No XML files generated. Exiting.");
                    return;
                }
                StartBatchManager(pathForXmlFile, logFolderPath);
                if (batchManagerStarted)
                {
                    string batchManagerErrorLogPath = "C:\\HQL\\Exe\\BatchManager\\IMPORTLOYALTYDOCUMENTSREPOSITORY.ERR";

                    if (File.Exists(batchManagerErrorLogPath) && new FileInfo(batchManagerErrorLogPath).LastWriteTime > Process.GetCurrentProcess().StartTime)
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "Batch Manager error log", logFolderPath);

                        Console.WriteLine("Success log not generated as the required XML file is missing.");
                    }
                }
                //else
                //{
                //    Console.WriteLine("Batch Manager failed to import XML data. Success log not generated.");
                //}

                Timer xmlProcessingTimer = new Timer(state => MonitorProcessedXmlFiles(pathForXmlFile, logFolderPath), null, TimeSpan.Zero, TimeSpan.FromMinutes(1));
                Environment.Exit(0);
            }

            static bool IsSuccessLogRequired(string pathForXmlFile)
            {
                string[] xmlFiles = Directory.GetFiles(pathForXmlFile, "ImportLoyaltyDocumentsRepository-????-??-??T??????-??.xml.scc");
                return xmlFiles.Length > 0;
            }

            static void LogErrorAndCopyBatchManagerErrorLog(string csvFilePath, string errorMessage, string logFolderPath)
            {
                string currentDateFormatted = DateTime.Now.ToString("yyyyMMdd");
                string logFileName = Path.Combine(logFolderPath, $"ErrorLog_Document_{currentDateFormatted}{GetSerialNumber()}.txt");

                string logMessage = $"{DateTime.Now} - Error processing file: {csvFilePath}. Error: {errorMessage}{Environment.NewLine}";

                string batchManagerErrorLogPath = "C:\\HQL\\Exe\\BatchManager\\IMPORTLOYALTYDOCUMENTSREPOSITORY.ERR";

                if (File.Exists(batchManagerErrorLogPath) && new FileInfo(batchManagerErrorLogPath).LastWriteTime > Process.GetCurrentProcess().StartTime)
                {
                    string batchManagerErrorLogContent = File.ReadAllText(batchManagerErrorLogPath);
                    string currentCsvErrorLog = ExtractErrorLogByDateTime(batchManagerErrorLogContent);

                    if (!string.IsNullOrEmpty(currentCsvErrorLog))
                    {
                        logMessage += $"Batch Manager error log:{Environment.NewLine}{currentCsvErrorLog}{Environment.NewLine}";
                    }
                    else
                    {
                        logMessage += "No Batch Manager error log data found for the current date and time.";
                    }
                }
                else
                {
                    logMessage += "Batch Manager error log file not found.";
                }

                File.AppendAllText(logFileName, logMessage);
                Console.WriteLine("Errors logged and Batch Manager error log copied.");
            }

            static string ExtractErrorLogByDateTime(string batchManagerErrorLog)
            {
                string currentDateFormatted = DateTime.Now.ToString("yyyy-MM-dd ");
                List<int> startIndexes = new List<int>();
                List<int> endIndexes = new List<int>();
                int startIndex = batchManagerErrorLog.IndexOf($"Start {currentDateFormatted}");
                int endIndex = batchManagerErrorLog.IndexOf($"End {currentDateFormatted}");
                while (startIndex != -1 && endIndex != -1)
                {
                    startIndexes.Add(startIndex);
                    endIndexes.Add(endIndex);
                    startIndex = batchManagerErrorLog.IndexOf($"Start {currentDateFormatted}", endIndex);
                    endIndex = batchManagerErrorLog.IndexOf($"End {currentDateFormatted}", endIndex + 1);
                }
                if (startIndexes.Count > 0 && endIndexes.Count > 0)
                {
                    StringBuilder sb = new StringBuilder();
                    int i = startIndexes.Count;
                    /*
                    for (int i = 0; i < startIndexes.Count; i++)
                    { */
                    int logLength = endIndexes[i - 1] - startIndexes[i - 1];
                    string currentErrorLog = batchManagerErrorLog.Substring(startIndexes[i - 1], logLength);
                    sb.AppendLine(currentErrorLog);
                    /*}*/
                    return sb.ToString();
                }
                else
                {
                    return "No error log data found for the current date.";
                }
            }

            static string GetSerialNumber()
            {

                int counter = 1;
                string serialNumberFilePath = "serial_number.txt";
                if (File.Exists(serialNumberFilePath))
                {
                    string currentSerialNumber = File.ReadAllText(serialNumberFilePath);
                    if (int.TryParse(currentSerialNumber, out counter))
                    {
                        counter++; // Increment the counter
                    }
                }
                File.WriteAllText(serialNumberFilePath, counter.ToString());
                return counter.ToString("D2");
            }

            static void EnsureFolderExists(string folderPath)
            {
                if (!Directory.Exists(folderPath))
                {
                    Directory.CreateDirectory(folderPath);
                    Console.WriteLine($"Created folder: {folderPath}");
                }
            }

            static void MonitorProcessedXmlFiles(string pathForXmlFile, string logFolderPath)

            {
                string[] xmlFiles = Directory.GetFiles(pathForXmlFile, "*.xml");

                foreach (var xmlFilePath in xmlFiles)
                {
                    string fileName = Path.GetFileName(xmlFilePath);
                    string logFileName = Path.ChangeExtension(fileName, ".log");
                    string warningLogFileName = Path.ChangeExtension(fileName, ".wrn");
                    string errorLogFileName = Path.ChangeExtension(fileName, ".err");
                    string logFilePath = Path.Combine(logFolderPath, logFileName);
                    string warningLogFilePath = Path.Combine(logFolderPath, warningLogFileName);
                    string errorLogFilePath = Path.Combine(logFolderPath, errorLogFileName);
                    if (File.Exists(logFilePath) || File.Exists(warningLogFilePath) || File.Exists(errorLogFilePath))
                    {
                        if (File.Exists(logFilePath))
                        {
                            Console.WriteLine($"File {fileName} has been processed successfully.");
                        }
                        else if (File.Exists(warningLogFilePath))
                        {
                            Console.WriteLine($"File {fileName} has been partially processed with warnings.");
                        }
                        else if (File.Exists(errorLogFilePath))
                        {
                            Console.WriteLine($"File {fileName} has encountered errors during processing.");
                        }
                    }
                }
            }

            static bool IsFileNameLegal(string fileName, string logFolderPath)
            {
                string pattern = @"Document{1}_\d{8}(\d{2})\.csv";
                if (!Regex.IsMatch(fileName, pattern))
                {
                    Console.WriteLine($"File name '{fileName}' does not match the expected format.");
                    LogErrorAndCopyBatchManagerErrorLog(fileName, "File name does not match the expected format.", logFolderPath);
                    return false;
                }
                return true;
            }

            static void GenerateXmlDocuments(int documentsPerXml, string csvFilePath, string pathForXmlFile, string archiveFolderPath, string logFolderPath)
            {
                int documentCount = 0;
                int fileCount = 1;
                int totalRecordsProcessed = 0;
                string currentDateTime = DateTime.Now.ToString("yyyy-MM-ddTHHmmss");
                using (StreamReader reader = new StreamReader(csvFilePath))
                {
                    // Read the header rows and skip them
                    string headerLine = reader.ReadLine(); // Read and skip the header line                                                     
                    if (string.IsNullOrEmpty(headerLine))
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "CSV file header is empty.", logFolderPath);
                        return; // Stop processing this CSV file
                    }
                    string[] columns = headerLine.Split(',').Select(c => c.Trim()).ToArray();
                    if (columns.Length != 3)
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "CSV file header doesn't contain the expected number of columns.", logFolderPath);
                        return; // Stop processing this CSV file
                    }

                    XmlDocument loyaltyDocument = new XmlDocument();
                    XmlElement loyaltyDocumentsRepository = loyaltyDocument.CreateElement("LoyaltyDocumentsRepository");
                    XmlElement documentsNode = null;
                    string linewithData = reader.ReadLine();
                    string[] values = linewithData.Split(',');
                    string retailerId = values[0].Trim();
                    string couponId = values[1].Trim();
                    string barcodeId = values[2].Trim();
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        try
                        {
                            line = line.Trim().Remove(line.IndexOf(','));
                        }
                        catch (Exception ex)
                        {
                            line = line.Trim();
                        }

                        if (string.IsNullOrEmpty(line))
                            continue;


                        //Check if retailer, coupon, and barcode are valid separately
                        if (!ValidateRetailer(retailerId))
                        {
                            Console.WriteLine("GenerateXmlDocuments Line Number 260");
                            LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Retailer ID {retailerId} is invalid.", logFolderPath);
                            return; // Stop processing this CSV file
                        }

                        if (!ValidateCoupon(couponId))
                        {
                            LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Coupon ID {couponId} is invalid.", logFolderPath);
                            return; // Stop processing this CSV file
                        }

                        if (!ValidateBarcode(barcodeId))
                        {
                            LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Barcode ID {barcodeId} is invalid.", logFolderPath);
                            return; // Stop processing this CSV file
                        }
                        if (documentsNode == null)
                        {
                            documentsNode = loyaltyDocument.CreateElement("Documents");
                            documentsNode.SetAttribute("ClubId", "1");
                            documentsNode.SetAttribute("RetailerId", retailerId);
                            documentsNode.SetAttribute("BarcodeId", barcodeId);
                            documentsNode.SetAttribute("Type", "3");
                            documentsNode.SetAttribute("CouponId", couponId);
                            loyaltyDocumentsRepository.AppendChild(documentsNode);
                        }
                        if (documentCount <= documentsPerXml)
                        {
                            // Create Document element
                            XmlElement documentNode = loyaltyDocument.CreateElement("Document");
                            documentNode.SetAttribute("Barcode", line);
                            documentsNode.AppendChild(documentNode);
                            documentCount++;
                        }
                        if (documentCount >= documentsPerXml)
                        {
                            FileName = $"ImportLoyaltyDocumentsRepository-{currentDateTime}-{fileCount:D2}.xml";
                            if (fileCount > 99)
                            {
                                // Take a new currentDateTime to ensure a different filename
                                currentDateTime = DateTime.Now.ToString("yyyy-MM-ddTHHmmss");
                                fileCount = 1;
                                FileName = $"ImportLoyaltyDocumentsRepository-{currentDateTime}-{fileCount:D2}.xml";
                            }

                            string filePath = Path.Combine(pathForXmlFile, FileName);
                            loyaltyDocument.AppendChild(loyaltyDocumentsRepository);
                            loyaltyDocument.Save(filePath);
                            Console.WriteLine($"Saved {filePath}");
                            fileCount++;
                            documentCount = 0;
                            documentsNode = null;
                            loyaltyDocument = new XmlDocument();
                            loyaltyDocumentsRepository = loyaltyDocument.CreateElement("LoyaltyDocumentsRepository");
                        }
                    }
                    if (documentCount < documentsPerXml)
                    {
                        currentDateTime = DateTime.Now.ToString("yyyy-MM-ddTHHmmss");
                        FileName = $"ImportLoyaltyDocumentsRepository-{currentDateTime}-{fileCount:D2}.xml";
                        if (fileCount > 99)
                        {
                            currentDateTime = DateTime.Now.ToString("yyyy-MM-ddTHHmmss");
                            fileCount = 1;
                            FileName = $"ImportLoyaltyDocumentsRepository-{currentDateTime}-{fileCount:D2}.xml";
                        }

                        string filePath = Path.Combine(pathForXmlFile, FileName);
                        loyaltyDocument.AppendChild(loyaltyDocumentsRepository);
                        loyaltyDocument.Save(filePath);
                        Console.WriteLine($"Saved {filePath}");
                        fileCount++;
                        documentCount = 0;
                        documentsNode = null;
                        loyaltyDocument = new XmlDocument();
                        loyaltyDocumentsRepository = loyaltyDocument.CreateElement("LoyaltyDocumentsRepository");
                    }

                    cvfilecount++;
                }
                string currentTime = DateTime.Now.ToString("HHmmss");
                string originalFileName = Path.GetFileNameWithoutExtension(csvFilePath); // Get the original file name without extension
                string fileExtension = Path.GetExtension(csvFilePath); // Get the file extension
                string newFileName = $"{originalFileName}_{currentTime}{fileExtension}";
                string newFilePath = Path.Combine(archiveFolderPath, newFileName);
                File.Move(csvFilePath, newFilePath);
                Console.WriteLine($"Moved {csvFilePath} to {newFilePath}");
            }

            static bool ValidateRetailer(string retailerId)
            {

                string connectionString = ConfigurationManager.ConnectionStrings["ImportCoupon"].ConnectionString;
                 Console.WriteLine("connectionString is "+ connectionString);
                string query = "SELECT COUNT(*) FROM Promotion_C4TW_UAT.dbo.RetailerCode_MP  WHERE MatrixMemberId  = @RetailerId";
                Console.WriteLine(query);
                if (connectionString != null && query != null)
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        using (SqlCommand command = new SqlCommand(query, connection))
                        {
                            // Add parameters
                            Console.WriteLine(retailerId);
                            command.Parameters.AddWithValue("@RetailerId", retailerId);
                            connection.Open();
                            int count = (int)command.ExecuteScalar();
                            Console.WriteLine(count);
                            return count > 0;

                        }
                    }
                }
                else
                {
                    // Log an error if connectionString or query is null
                    Console.WriteLine("Connection string or query is null.");
                    return false;
                }
            }

            static bool ValidateCoupon(string couponId)
            {
                // Example: Check if the coupon ID exists in a database
                string connectionString = ConfigurationManager.ConnectionStrings["ImportCoupon"].ConnectionString;               
                string selectQuery = ConfigurationManager.AppSettings["ValidateCouponQuery"];
                if (connectionString != null && selectQuery != null)
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        using (SqlCommand command = new SqlCommand(selectQuery, connection))
                        {
                            command.Parameters.AddWithValue("@CouponId", couponId);
                            connection.Open();
                            int count = (int)command.ExecuteScalar();
                            return count > 0;
                        }
                    }
                }
                else
                {
                    // Log an error if connectionString or query is null
                    Console.WriteLine("Connection string or query is null.");
                    return false;
                }

            }

            static bool ValidateBarcode(string barcodeId)
            {
                Console.WriteLine(barcodeId);
                // Example: Check if the barcode ID exists in a database
                string connectionString = ConfigurationManager.ConnectionStrings["ImportCoupon"].ConnectionString;
                string query = "SELECT COUNT(*) FROM Promotion_C4TW_UAT.dbo.PromotionDocuments WHERE BarcodeId = @BarcodeId";
                if (connectionString != null && query != null)
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        using (SqlCommand command = new SqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@BarcodeId", barcodeId);
                            connection.Open();
                            int count = (int)command.ExecuteScalar();
                            Console.WriteLine(count);
                            return count > 0;
                        }
                    }
                }
                else
                {
                    // Log an error if connectionString or query is null
                    Console.WriteLine("Connection string or query is null.");
                    return false;
                }
            }

            static void StartBatchManager(string pathForXmlFile, string logFolderPath)
            {
                bool allFilesProcessedSuccessfully = true;
                string[] csvFiles = Directory.GetFiles(ConfigurationManager.AppSettings["PathForImportExternalCoupon"], "*.csv");
                foreach (var csvFilePath in csvFiles)
                {
                    if (!IsFileNameLegal(csvFilePath, logFolderPath))
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "NCR3 Illegal file name detected", logFolderPath);
                        allFilesProcessedSuccessfully = false;
                        continue; // Move to the next CSV file
                    }
                    if (!ProcessCsvFile(csvFilePath, logFolderPath))
                    {
                        allFilesProcessedSuccessfully = false;
                        break;
                    }
                }
                if (allFilesProcessedSuccessfully)
                {
                    try
                    {
                        RunREmaBatchmanagerCommand(pathForXmlFile, logFolderPath);
                        batchManagerStarted = true;
                        if (IsSuccessLogRequired(pathForXmlFile))
                        {
                            GenerateSuccessLog(logFolderPath, pathForXmlFile);
                            Console.WriteLine("Success log generated.");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to start Batch Manager: {ex.Message}");
                        Console.WriteLine("Retrying tomorrow.");
                        // Schedule retry for the next day
                        ScheduleRetry(pathForXmlFile, logFolderPath);
                    }
                }
            }
            static bool ProcessCsvFile(string csvFilePath, string logFolderPath)
            {
                string[] lines = File.ReadAllLines(csvFilePath);
                if (lines.Length == 0)
                {
                    LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "CSV file is empty.", logFolderPath);
                    return false; // Stop processing this CSV file
                }
                string headerLine = lines[0];
                if (string.IsNullOrEmpty(headerLine))
                {

                    return false; // Stop processing this CSV file
                }
                string[] columns = headerLine.Split(',').Select(c => c.Trim()).ToArray();
                if (columns.Length != 3)
                {
                    LogErrorAndCopyBatchManagerErrorLog(csvFilePath, "CSV file header doesn't contain the expected number of columns.", logFolderPath);
                    return false; // Stop processing this CSV file
                }
                for (int i = 1; i < lines.Length; i++)
                {
                    string line = lines[i];
                    string[] data = line.Split(',').Select(c => c.Trim()).ToArray();
                    if (data.Length != columns.Length)
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Row {i + 1} doesn't contain the expected number of columns.", logFolderPath);
                        return false; // Stop processing this CSV file
                    }
                    string retailerId = columns[0];
                    string couponId = columns[1];
                    string barcodeId = columns[2];
                    if (!ValidateRetailer(retailerId))
                    {
                        Console.WriteLine("ProcessCsvFile Line Number 506");
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Retailer ID {retailerId} is invalid.", logFolderPath);
                        return false; // Stop processing this CSV file
                    }

                    if (!ValidateCoupon(couponId))
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Coupon ID {couponId} is invalid.", logFolderPath);
                        return false; // Stop processing this CSV file
                    }

                    if (!ValidateBarcode(barcodeId))
                    {
                        LogErrorAndCopyBatchManagerErrorLog(csvFilePath, $"Barcode ID {barcodeId} is invalid.", logFolderPath);
                        return false; // Stop processing this CSV file
                    }
                }
                string currentDateTime = DateTime.Now.ToString("yyyyMMddHHmmss");
                string newFileName = $"Document_{currentDateTime}_{Path.GetFileName(csvFilePath)}";
                string archiveFolderPath = ConfigurationManager.AppSettings["PathForImportExternalCouponFileArchive"];
                string newFilePath = Path.Combine(archiveFolderPath, newFileName);
                File.Move(csvFilePath, newFilePath);
                Console.WriteLine($"Moved {csvFilePath} to {newFilePath}");
                return true;
            }

            static void ScheduleRetry(string pathForXmlFile, string logFolderPath)
            {
                // Schedule retry for the next day
                DateTime tomorrow = DateTime.Today.AddDays(1);
                TimeSpan timeUntilTomorrow = tomorrow - DateTime.Now;
                Timer timer = new Timer(state => RetryCallback(pathForXmlFile, logFolderPath), null, timeUntilTomorrow, TimeSpan.FromDays(1));
            }
            static void RetryCallback(object state, string logFolderPath)
            {
                if (!batchManagerStarted)
                {
                    var args = (Tuple<string, string>)state;
                    Console.WriteLine("Retrying to start Batch Manager...");
                    StartBatchManager(args.Item1, args.Item2);
                }
            }
            static void RunREmaBatchmanagerCommand(string pathForXmlFile, string logFolderPath)
            {

                string REMAFolderPath = ConfigurationManager.AppSettings["REMALocation"];

                string batchFilePath = ConfigurationManager.AppSettings["REMAbatchLocation"];

                if (!File.Exists(batchFilePath))
                {
                    Console.WriteLine($"Error: Batch file '{batchFilePath}' not found.");
                    return;
                }
                ProcessStartInfo processStartInfo = new ProcessStartInfo(batchFilePath);

                processStartInfo.WorkingDirectory = REMAFolderPath;

                processStartInfo.RedirectStandardOutput = true;
                processStartInfo.UseShellExecute = false;
                Process process = Process.Start(processStartInfo);
                process.WaitForExit();
                Console.WriteLine("REmaBatchmanager command executed.");

            }
            static void GenerateSuccessLog(string logFolderPath, string pathForXmlFile)
            {
                string[] xmlFiles = Directory.GetFiles(pathForXmlFile, "ImportLoyaltyDocumentsRepository-????-??-??T??????-??.xml.scc");
                // Filter the .scc files generated after the application's execution
                xmlFiles = xmlFiles.Where(file => new FileInfo(file).LastWriteTime > Process.GetCurrentProcess().StartTime).ToArray();

                // Check if any .scc files exist
                if (xmlFiles.Length > 0)
                {
                    // Get the .scc file with the latest timestamp
                    string latestXmlFile = xmlFiles.OrderByDescending(f => new FileInfo(f).LastWriteTime).First();
                    string fileName = Path.GetFileName(latestXmlFile);

                    // Log the latest .scc file in the success log
                    string currentDateFormatted = DateTime.Now.ToString("yyyy-MM-dd");
                    string logFileName = Path.Combine(logFolderPath, $"success_log_{currentDateFormatted}.txt");
                    string successLogContent = $"{DateTime.Now} - Import completed successfully: {FileName}";
                    File.AppendAllText(logFileName, successLogContent + Environment.NewLine);
                }
                else
                {
                    Console.WriteLine("No .scc files found in the directory. Skipping success log generation.");
                }
            }

        }
    }
}