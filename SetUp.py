#from Singleton import Singleton
class SetUp():
    def __init__(self):
        print("Getting Environment Variables")
        self.SqlOdbcDriver  = "ODBC Driver 17 for SQL Server"
        self.DB_Server_NAME = "BPLDEVDB01"
        self.DBName_CI      = "VishnuR_prepaid_CI"
        self.SMTP_SERVER    = "corecard-com.mail.protection.outlook.com"
        self.SMTP_PORT      = 25
        self.EmailFrom      = ""
        self.Temp_EmailTo   = ""

    @classmethod
    def IPMClearingAndSettlement(cls):
        cls.IPMFileIN = "E:/Project/PrepaidProcessing/Dump/IPM_masterCard_Python/IPMClr/IN/"
        cls.IPMFileOUT = "E:/Project/PrepaidProcessing/Dump/IPM_masterCard_Python/IPMClr/OUT/"
        cls.IPMFileError = "E:/Project/PrepaidProcessing/Dump/IPM_masterCard_Python/IPMClr/ERROR/"
        cls.IPMFileLog = "E:/Project/PrepaidProcessing/Dump/IPM_masterCard_Python/IPMClr/LOG/"
        cls.IPMFileSequence = 'A004,A005,A006,A001'
        cls.TxnInsertToDB = 10000
        cls.IPM_ValidationEnable = 1
        cls.IPM_UseCCardOrCCard2 = 0
        #Time in Seconds
        cls.FileRecheckTime = 2
        #Time in Seconds
        cls.FileSizeRecheckTime = 2
        # False = Disable or No or 0 / True = Enable or Yes or 1
        cls.LogEnable = False
        cls.IsAgingReq = True
        # Blocked File Processing = True (Original IPM File) / UnBlocked File Process = False ( PERF IPM File)
        cls.IsBlockedFile = True
        # Prod Env = True (Original IPM File) / Local / Non-Prod Env = False
        cls.IsProdEnv = False
        return cls