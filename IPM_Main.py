import sys
import os
import time
import ctypes
import datetime
import tracemalloc
from SetUp import SetUp
from Logger import Logger
from MCIPM_Parser import IpmReader
import Get_ISOSubFields as FP_Values
import multiprocessing
import SQL_Connections as SQL_Connect
import IPM_Select_And_Updates
import Functions
import IPM_SPCall
import singleton


######################################################################################################################################################

if __name__ == "__main__":
    # Main
    multiprocessing.freeze_support()
    ctypes.windll.kernel32.SetConsoleTitleW("IPM Clearing And Settlement Module")
    
    try:
        print("CHECKING INSTANCE")
        multi_check = singleton.SingleInstance()
    except:
        print("Another Module is Already Running")
        sys.exit(-1)
    
    ######################################################################################################################################################

    global Parse_rec_count, FileInError, Inp_Jobid, EnvVariable_Dict, Rec_Count, GotoReadFile, IsProdFileName, FileDateFromFile, ErrorReason

    logger = Logger()
    IpmReader_Instance = IpmReader(logger)
    print(datetime.datetime.now())
    st = datetime.datetime.now()
    EnvVar = SetUp()
    FileSource = 'MASTERCARDIPM'
    Sub_FileType = 'Clearing And Settlement'
    
    try:
        EnvVariable_Dict = {
                                "DB_Server_NAME"          : EnvVar.DB_Server_NAME.strip(),\
                                "DBName_CI"               : EnvVar.DBName_CI.strip(),\
                                "SqlOdbcDriver"           : EnvVar.SqlOdbcDriver.strip(),\
                                "SMTP_SERVER"             : EnvVar.SMTP_SERVER.strip(),\
                                "SMTP_PORT"               : EnvVar.SMTP_PORT,\
                                "EmailFrom"               : EnvVar.EmailFrom.strip(),\
                                "Temp_EmailTo"            : EnvVar.Temp_EmailTo.strip(),\
                                "InputDir"                : EnvVar.IPMClearingAndSettlement().IPMFileIN.strip(),\
                                "OutputDir"               : EnvVar.IPMClearingAndSettlement().IPMFileOUT.strip(),\
                                "ErrorDir"                : EnvVar.IPMClearingAndSettlement().IPMFileError.strip(),\
                                "LogDir"                  : EnvVar.IPMClearingAndSettlement().IPMFileLog.strip(),\
                                "IPM_ValidationEnable"    : EnvVar.IPMClearingAndSettlement().IPM_ValidationEnable,\
                                "IPM_UseCCardOrCCard2"    : EnvVar.IPMClearingAndSettlement().IPM_UseCCardOrCCard2,\
                                "TxnInsertToDB"           : EnvVar.IPMClearingAndSettlement().TxnInsertToDB,\
                                "IPMFileSequence"         : EnvVar.IPMClearingAndSettlement().IPMFileSequence,\
                                "FileRecheckTime"         : EnvVar.IPMClearingAndSettlement().FileRecheckTime,\
                                "FileSizeRecheckTime"     : EnvVar.IPMClearingAndSettlement().FileSizeRecheckTime,\
                                "IsAgingReq"              : EnvVar.IPMClearingAndSettlement().IsAgingReq,\
                                "IsBlockedFile"           : EnvVar.IPMClearingAndSettlement().IsBlockedFile,\
                                "IsProdEnv"               : EnvVar.IPMClearingAndSettlement().IsProdEnv
                            }
    except Exception as e:
        print(e)
        sys.exit()

    for key,value in EnvVariable_Dict.items():
        logger.debug(f"{key} : {value}")
        
    EmailTo = []
    EmailTo = EnvVariable_Dict['Temp_EmailTo'].split(",")
    AgingFlag = 1 if EnvVariable_Dict.get('IsAgingReq') == True else 0
    Connection_String = SQL_Connect.udf_GetConnectionString(EnvVariable_Dict['SqlOdbcDriver'], EnvVariable_Dict['DB_Server_NAME'], EnvVariable_Dict['DBName_CI'])
    
    logger.info("*************************** Clearing And Settlement Processing Starts ***************************",True)
    
    while True:
        logger.info("*************************** Going To Check For File ***************************",True)
        GoToProcessFile = "Any"

        res = len([name for name in os.listdir(EnvVariable_Dict.get('InputDir')) if os.path.isfile(os.path.join(EnvVariable_Dict.get('InputDir'), name))])

        GoToProcessFile = Functions.process_files_size_check(EnvVariable_Dict.get('InputDir'),EnvVariable_Dict.get('FileSizeRecheckTime'),EnvVariable_Dict.get('ErrorDir')) if res > 0 else False
        
        if GoToProcessFile:

            Rec_Count = IPM_Select_And_Updates.IPM_Select(1, Connection_String, ArgVar_1 = FileSource)
                       
            if Rec_Count[0][0] > 0: 
                GoToProcessFile = False
                logger.debug("File Status in ClearingFiles is not as per expectation for MASTERCardIPM Clearing File",True)
            
            if GoToProcessFile == True :
                time.sleep(EnvVariable_Dict['FileSizeRecheckTime'])
                
                logger.info("*************************** FILE FOUND PROCESSING STARTS ***************************",True)
                
                tracemalloc.start()
                    
                logger.info(f"Initially Input Folder = {EnvVariable_Dict.get('InputDir')}")
                file_list = [name for name in os.listdir(EnvVariable_Dict.get('InputDir')) if os.path.isfile(os.path.join(EnvVariable_Dict.get('InputDir'), name)) ]
                file_list.sort(key=lambda s: os.path.getmtime(os.path.join(EnvVariable_Dict.get('InputDir'), s)))

                logger.info(f"TotalFileCount In Folder = {len(file_list)}")
                
                current,peak = tracemalloc.get_traced_memory()
                logger.info("Before Start Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                tracemalloc.clear_traces()

                for fname in file_list:
                    InFileName = fname
                    InFilePath = f"{EnvVariable_Dict.get('InputDir')}{InFileName}"
                    
                    res = Functions.check_file_out_dir(EnvVariable_Dict.get('OutputDir'),InFileName)
                    if res: Functions.File_Movement(InFilePath,EnvVariable_Dict.get('ErrorDir'),1)
                    
                    FileHash = Functions.Gen_FileHash(InFilePath)
                    
                    logger.info(f"Processing Filename is : {InFileName} and Processing FilePath is : {InFilePath}",True)
                    
                    Upd_InFileName = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{InFileName}"
                    Functions.change_file_name(InFilePath,Upd_InFileName)
                    
                    InFilePath = f"{EnvVariable_Dict.get('InputDir')}{Upd_InFileName}"
                    
                    Functions.File_Movement(InFilePath,EnvVariable_Dict.get('OutputDir'))
                    OutFilePath = EnvVariable_Dict.get('OutputDir') + Upd_InFileName
                    logger.info(f"Out File Path : {OutFilePath}",True)
                    
                    res = IPM_Select_And_Updates.CreateJobIntoClearingFiles(Connection_String, InFileName, OutFilePath, Upd_InFileName, FileHash, FileSource)
                        
                    res = IPM_SPCall.SP_Call_ChangeFileStatus(Connection_String, EnvVariable_Dict.get('IPMFileSequence'))
                    GotoReadFile = True if res == 1 else False

                    current,peak = tracemalloc.get_traced_memory()
                    logger.info("VALIDATION Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                    tracemalloc.clear_traces()
                    
                    while GotoReadFile:
                        res = IPM_Select_And_Updates.IPM_Select(2, Connection_String, ArgVar_1 = FileSource)

                        if res[0][0] != 1:
                            logger.error("More than 1 Or No file has filestatus VALIDATION Check Log and SP")
                            sys.exit()
                        else:
                            res             = IPM_Select_And_Updates.IPM_Select(3, Connection_String, ArgVar_1 = FileSource)
                            Inp_Jobid       = res[0][0]
                            InputFilePath   = res[0][1]
                            INFileName      = res[0][2]
                            
                            Stage_StartTime = datetime.datetime.now()
                            Ins_StartTime = Stage_StartTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            #IPM_Select_And_Updates.IPM_Update(6, Connection_String, Inp_Jobid, ArgVar_1 = 'InQueue')
                            #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)

                            #logger.info("**************************************** VALIDATION STARTS ****************************************",True)
                            
                            '''res = File_Processer.File_Validate(InputFilePath)
                            IsUnexpectedMTI = res[0]
                            FileInError     = res[1]
                            ErrorReason     = res[2]
                            OtherRecCount   = res[3]
                            Head_Trail_Error = res[4]'''

                            logger.debug('Going To Truncate Table IPMMaster_Interim',True)
                            InsQuery = "TRUNCATE TABLE IPMMaster_Interim"
                            SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                            logger.debug('IPMMaster_Interim Truncated',True)
                            
                            logger.info("**************************************** VALIDATION END & PARSING STARTS ****************************************")
                            
                            current,peak = tracemalloc.get_traced_memory()
                            logger.info("Validation End Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                            tracemalloc.clear_traces()
                            
                            Stage_EndTime = datetime.datetime.now()
                            TimeDiff = str(Stage_EndTime - Stage_StartTime)
                            TimeTaken = f"VALIDATION {TimeDiff[:-3]}"
                            IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'PARSING','VALIDATION', TimeTaken, To = 3, From = 1)
                            #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                            Stage_StartTime = datetime.datetime.now()
                                                    
                            MainTableColumn = ['TranId' ,'MessageTypeIdentifier' ,'ProcCode' ,'ProcCodeFromAccType' ,'ProcCodeToAccType' ,'TransactionAmount' ,'TxnSrcAmt' ,'SettlementAmount' ,'ConversionRateSettlement' ,'AmountCurrencyConversionAssessment' ,'DateLocalTransaction' ,'TimeLocalTransaction' ,'ExpirationDate' ,'POSInputCap' ,'POSCardHolderAuthCap' ,'POSCardCaptureCapabilities' ,'POSTermOpEnv' ,'POSCardholderPresenceInd' ,'POSCardPresenceIndicator' ,'POSCardDataInputMode' ,'POSCardmemberAuth' ,'POSCardmemberAuthEntity' ,'POSCardDataOutputCapability' ,'POSTerminalOutputCapability' ,'PINEntryCapability' ,'CardSequenceNumber' ,'FunctionCode' ,'MessageReasonCode' ,'AmountOrgRecon' ,'MerchantType' ,'AmountOrgTran' ,'AcquirerID' ,'SeqNumAcqDet' ,'CheckDigit' ,'JulianProSubDate' ,'AcquiringInsitutionIDCode' ,'ForwardingInsitutionIDCode' ,'RetrievalReferenceNumber' ,'ApprovalCode' ,'ServiceCode' ,'CardAcceptorTerminalID' ,'CardAcceptorIdCode' ,'MerchantName' ,'MerchantStreetAddress' ,'MerchantCity' ,'MerchantLocPostalCode' ,'MerchantStProvCode' ,'MerchantStateProvinceCode' ,'MerchantCountry' ,'MerchantCountryCode' ,'TransactionCurrencyCode' ,'SettlementCurrencyCode' ,'CardholderBillingAmount' ,'ConvRateCardholderBilling' ,'InterchangeFeeIndicator' ,'ConditionID' ,'AccountType' ,'AmountType' ,'CurrencyCode' ,'AmountSign' ,'Amount' ,'AdditionalAmount' ,'PrimaryCurrencyCode' ,'SecondaryCurrencyCode' ,'TertiaryCurrencyCode' ,'AccountType1' ,'AmountType1' ,'CurrencyCode1' ,'AmountSign1' ,'Amount1' ,'AccountType2' ,'AmountType2' ,'CurrencyCode2' ,'AmountSign2' ,'Amount2' ,'AmountType3' ,'AccountType3' ,'CurrencyCode3' ,'AmountSign3' ,'Amount3' ,'AccountType4' ,'AmountType4' ,'CurrencyCode4' ,'AmountSign4' ,'Amount4' ,'AccountType5' ,'AmountType5' ,'CurrencyCode5' ,'AmountSign5' ,'Amount5' ,'ApplicationCryptogram' ,'CryptogramInformationData' ,'IssuerApplicationData' ,'UnpredictableNumber' ,'ApplicationTxnCounter' ,'TerminalVerificationResult' ,'Transactiondate' ,'TransactionType' ,'AuthorizedAmount' ,'TransactionCurrencyCodeICC' ,'ApplicationInterchangeProfile' ,'TerminalCountryCode' ,'OtherAmount' ,'TerminalType_ICC9F35' ,'TerminalApplicationversionNumber_ICC9F09' ,'InterfaceDeviceSerialNumber_ICC9F1E' ,'TerminalCapabilities_ICC9F33' ,'CardholderVerificationMethodResults_ICC9F34' ,'TransactionSequenceCounter_ICC9F41' ,'TransactionCategoryCode_ICC9F53' ,'DedicatedFileName_ICC84' ,'ICCSysRelatedData' ,'IssuerAppData' ,'LifeCycleSupportIndicator' ,'MessageNumber' ,'BankNetDate' ,'DataRecordInitial' ,'ActionDate' ,'CardIssuerRefData' ,'AccountTypePDS' ,'CurrCodeCardHolderBilling' ,'JobStatus' ,'TranTime' ,'PostTime' ,'CaseID' ,'DriverNumberIDNumber' ,'MessageDirection' ,'PartialAmountIndicator' ,'OutgoingStatus' ,'TransacOriginatorInstIDCode' ,'CardAcceptorNameLocation' ,'ResponseCode' ,'NetworkReferenceID' ,'TheFinancialNetworkCode' ,'AuthorizationResponseCode' ,'NetworkName' ,'TraceID' ,'CashBackAmount' ,'PaymentTransactionTypeIndicator' ,'ReceivingICANumber' ,'SendingICANumber' ,'gcmsproductidentifier' ,'mtMSMCAdditional' ,'RecordData' ,'ReceivingInstIDCode' ,'UniqueID' ,'JobId' ,'MotorFuelQuantity' ,'MotorFuelSaleAmount' ,'MotorFuelUnitPrice' ,'StateCountryCode' ,'TokenRequestorID' ,'VehicleNumber' ,'CurrencyCode13' ,'CreditDebit15' ,'Amount16' ,'CurrencyCode23' ,'CreditDebit25' ,'Amount26' ,'CurrencyCode33' ,'CreditDebit35' ,'Amount36' ,'CurrencyCode43' ,'CreditDebit45' ,'Amount46' ,'CurrencyCode53' ,'CreditDebit55' ,'Amount56' ,'CurrencyCode63' ,'CreditDebit65' ,'Amount66' ,'AuxillaryField1_00PDS' ,'AuxillaryField2_00PDS' ,'AuxillaryField1_01PDS' ,'AuxillaryField2_01PDS' ,'AuxillaryField1_02PDS' ,'AuxillaryField2_02PDS' ,'AuxillaryField1_03PDS' ,'AuxillaryField2_03PDS' ,'AuxillaryField1_04PDS' ,'AuxillaryField2_04PDS' ,'OriginalTerminalType' ,'TerminalType' ,'MessageReversalIndicator' ,'CentSiteProcDateOFile' ,'ProgramRegistrationID' ,'MessageReversalInd' ,'CentSiteProcDateOMess' ,'SecurityProtocol' ,'CardHolderAuthentication' ,'UCAFCollectionIndicator' ,'TxnCategoryIndicator' ,'MCElectronicCardIndicator' ,'TaxAmountRateTypeCode' ,'TaxAmountValueAddedTax' ,\
                                                'TaxAmountCurrencyCode' ,'TaxAmountCurrencyExponent' ,'TaxAmountDebitCreditIndicator' ,'FeeCollectionControlNumber' ,'TransmissionDateTime' ,'AlternateTransactionFee_CurrencyCode' ,'AlternateTransactionFee_Amount' ,'FeeTypeCodeTxnFee1' ,'FeeProcessCodeTxnFee1' ,'FeeSettleIndicator1' ,'CurrencyCodeFee1' ,'AmountFeeTxnFee1' ,'CurrencyCodeFeeRecon1' ,'AmountFeeRecon1' ,'FeeTypeCodeTxnFee2' ,'FeeProcessCodeTxnFee2' ,'FeeSettleIndicator2' ,'CurrencyCodeFee2' ,'AmountFeeTxnFee2' ,'CurrencyCodeFeeRecon2' ,'AmountFeeRecon2' ,'FeeTypeCodeTxnFee3' ,'FeeProcessCodeTxnFee3' ,'FeeSettleIndicator3' ,'CurrencyCodeFee3' ,'AmountFeeTxnFee3' ,'CurrencyCodeFeeRecon3' ,'AmountFeeRecon3' ,'FeeTypeCodeTxnFee4' ,'FeeProcessCodeTxnFee4' ,'FeeSettleIndicator4' ,'CurrencyCodeFee4' ,'AmountFeeTxnFee4' ,'CurrencyCodeFeeRecon4' ,'AmountFeeRecon4' ,'FeeTypeCodeTxnFee5' ,'FeeProcessCodeTxnFee5' ,'FeeSettleIndicator5' ,'CurrencyCodeFee5' ,'AmountFeeTxnFee5' ,'CurrencyCodeFeeRecon5' ,'AmountFeeRecon5' ,'FeeTypeCodeTxnFee6' ,'FeeProcessCodeTxnFee6' ,'FeeSettleIndicator6' ,'CurrencyCodeFee6' ,'AmountFeeTxnFee6' ,'CurrencyCodeFeeRecon6' ,'AmountFeeRecon6' ,'FeeTypeCodeTxnFee7' ,'FeeProcessCodeTxnFee7' ,'FeeSettleIndicator7' ,'CurrencyCodeFee7' ,'AmountFeeTxnFee7' ,'CurrencyCodeFeeRecon7' ,'AmountFeeRecon7' ,'FeeTypeCodeTxnFee8' ,'FeeProcessCodeTxnFee8' ,'FeeSettleIndicator8' ,'CurrencyCodeFee8' ,'AmountFeeTxnFee8' ,'CurrencyCodeFeeRecon8' ,'AmountFeeRecon8' ,'FeeTypeCodeTxnFee9' ,'FeeProcessCodeTxnFee9' ,'FeeSettleIndicator9' ,'CurrencyCodeFee9' ,'AmountFeeTxnFee9' ,'CurrencyCodeFeeRecon9' ,'AmountFeeRecon9' ,'FeeTypeCodeTxnFee10' ,'FeeProcessCodeTxnFee10' ,'FeeSettleIndicator10' ,'CurrencyCodeFee10' ,'AmountFeeTxnFee10' ,'CurrencyCodeFeeRecon10' ,'AmountFeeRecon10' ,'FeeTypeCodeTxnFee11' ,'FeeProcessCodeTxnFee11' ,'FeeSettleIndicator11' ,'CurrencyCodeFee11' ,'AmountFeeTxnFee11' ,'CurrencyCodeFeeRecon11' ,'AmountFeeRecon11' ,'FeeTypeCodeTxnFee12' ,'FeeProcessCodeTxnFee12' ,'FeeSettleIndicator12' ,'CurrencyCodeFee12' ,'AmountFeeTxnFee12' ,'CurrencyCodeFeeRecon12' ,'AmountFeeRecon12' ,'CurrencyExponent' ,'CurrencyCode1_148' ,'CurrencyExponent1' ,'CurrencyCode2_148' ,'CurrencyExponent2' ,'CurrencyCode3_148' ,'CurrencyExponent3' ,'CurrencyCode4_148' ,'CurrencyExponent4' ,'CurrencyCode5_148' ,'CurrencyExponent5' ,'CurrencyCode6' ,'CurrencyExponent6' ,'CurrencyCode7' ,'CurrencyExponent7' ,'CurrencyCode8' ,'CurrencyExponent8' ,'CurrencyCode9' ,'CurrencyExponent9' ,'CurrencyCode10' ,'CurrencyExponent10' ,'CurrencyCode11' ,'CurrencyExponent11' ,'CurrencyCode12' ,'CurrencyExponent12' ,'CurrencyExponent13' ,'CurrencyCode14' ,'CurrencyExponent14' ,'CurrencyCode15' ,'CurrencyExponent15' ,'CurrencyCodeOriginalTxnAmount' ,'CurrencyCodeOrgReconAmount' ,'AlternateProcessorIndicator' ,'MCAssignedIDOvrrideIndicator' ,'FutureUse1_PDS0044' ,'PDS0158FutureUse11' ,'PDS0158FutureUse13' ,'PDS0158FutureUse14' ,'AcceptanceBrandIDcode' ,'BusinessServiceIDCode' ,'BusinessServiceLevelCode' ,'mccoverrideindicator' ,'productoverrideindicator' ,'rateapplyindicator' ,'BusinessDate' ,'BusinessCycle' ,'SettlementAgreementInfo' ,'SettlementIndicator' ,'DocumentationIndicator' ,'deInterchangeRateIndicator_Out' ,'InterchangeRateIndicator' ,'CharacterSetIndicator' ,'AdditionalContactInfo' ,'MerchantTelephoneNumber' ,'MerchantDescriptionData' ,'SoleProprietorName' ,'LegalCorporateName' ,'DUN_Dun_Bradstreet' ,'CrossBorderIndicator' ,'CurrencyIndicator' ,'CharacterSetIndicator2' ,'CardAcceptorDataDesc' ,'CardAcceptorURL' ,'MerchantID' ,'TypeofInstallments' ,'NumberofInstallments' ,'InterestRatePDS0181' ,'FirstInstallmentAmountPDS0181' ,'SubsequentInstallmentAmountPDS0181' ,'AnnualPercentRatePDS0181' ,'InstallmentFeePDS0181' ,'CommissionRatePDS0181' ,'CommissionSignPDS0181' ,'CommissionAmountPDS0181' ,'BankNetReferenceNumber' ,'FormatNumber' ,'PhoneData' ,'PartnerIdCode' ,'PaymentTransactionInitiator' ,'OriginatingMessageFormat' ,'RemotePymtsProgData' ,'TNumberofInstallments' ,'InstallmentOption' ,'InstallmentNumber' ,'BonusCode' ,'BonusMonthCode' ,'NoOfPayementPerYr' ,'BonusAmount' ,'FirstMonthBonusPayement' ,'MobilePhoneNumber' ,'MPhoneServiceProvider' ,\
                                                    'TaxAmount1' ,'TaxAmount2' ,'TaxPercentage' ,'TaxBaseAmount' ,'TaxAmount3' ,'PrimaryAcctNbrSyntErr' ,'AmountSyntErr' ,'DataElementID' ,'ErrorSeverityCode' ,'ErrorMessageCode' ,'SubfieldID' ,'ATMLatePresentmentIndicator' ,'NbrDaysSinceTranOccurred' ,'WalletIdentifier' ,'TransitTranTypeIndicator' ,'TranspModeIndicator' ,'ConvertedToAccountNumber' ,'RetrievalDocumentCode' ,'MCControlNo' ,'DateFirstReturnBus' ,'EditExclResultsCode' ,'MCIssRetrievalReqDate' ,'MCAcqRetrievalRespCode' ,'MCAcqRetrievalRespSentDate' ,'MCIssuerResponseCode' ,'MCIssuerResponseDate' ,'MCIssuerRejectReasons' ,'MCImageReviewDecision' ,'MCImageReviewDate' ,'MCCbackSuppDocDate1' ,'MCCbackDocProcDate2P' ,'MasterComSenderMemo' ,'MasterComReceiverMemo' ,'MasterComImageReviewMemo_PDS0248' ,'MasterComRecordId' ,'MasterComSenderEndpointNbr' ,'MasterComRecieverEndpointNbr' ,'MasterComSystemEnhancedData' ,'MasterComMemberEnhancedData' ,'MasterComMessageType' ,'MCPreferedAcqrEndPoint' ,'ExclusionRequestCode' ,'ExclusionReasonCode' ,'ExclusionResultsCode' ,'InterchangeLifeCycleValidationCode' ,'DocIndicator' ,'InitialMessageReasonCode' ,'DateInitialPresentmtBussiness' ,'EditExclReasonCode' ,'AmountFirstReturn' ,'CurrCodeFirstReturn' ,'DataRecFirstReturn' ,'MessageReasonCodeSec' ,'DateSecondReturnBus' ,'AmountSecondReturn' ,'CurrCodeSecondReturn' ,'DataRecSecondReturn' ,'AmountPartialTransaction' ,'CurrencyCodePartialTransaction' ,'SettlementTransferAgentID' ,'SettleTransferAgentAcct' ,'SettlementLevelCode' ,'SettlementServiceIDCode' ,'SettleForeignExchRateClass' ,'SettlementDate' ,'SettlementCycle' ,'MemberReconIndicator1' ,'TranFeeAmtSyntErr' ,'CustomerServiceNumber' ,'MemberToMemberProprietary_PDS1000' ,'TotalTransactionNumber' ,'AmountTransaction' ,'AmtTranInTranCurr' ,'VirtualAccountNumber' ,'FileReversalIndicator' ,'SourceMessageNumberID' ,'CardProgramIdentifier' ,'BussinessServiceAgreementTypeCode' ,'deBusinessServiceIDCode' ,'SourceFileType' ,'SourceFileReferenceDate' ,'SourceProcessorID' ,'SourceFileSeqNumber' ,'PDSData' ,'ReconFileType' ,'ReconFileReferenceDate' ,'ReconProcessorID' ,'ReconFileSeqNumber' ,'ReconciledMemberActivity' ,'ReconAcceptBrandIdCode' ,'ReconBusiServiceLevelCode' ,'ReconBusinessServiceIDCode' ,'ReconInterchgRateIndicator' ,'ReconBusinessDate' ,'ReconBusinessCycle' ,'ReconFunctionCode' ,'ReconMsgTypeIdentifier' ,'ReconProcessingCode' ,'ReconSettleTransferAgentID' ,'ReconSettleTransAgentAcct' ,'ReconSettleLevelCode' ,'ReconSettleServiceIDCode' ,'ReconSettleExchgRateClass' ,'ReconReconciliationCycle' ,'ReconReconciliationDate' ,'ReconSettlementDate' ,'ReconSettlementCycle' ,'ReconciledCardProgramIdentifier' ,'ReconciledTransactionFunctionGroupCode' ,'ReconciledAcquirerBIN' ,'MCAssignedIDOvrrideIndicator2' ,'PDS0358FutureUse12' ,'PDS0358FutureUse13' ,'PDS0358FutureUse14' ,'BeginingAcctRangeID' ,'EndingAcctRangeID' ,'OriginalReversalTotalIndicator' ,'deCreditDebitIndicator1_Out' ,'deCreditDebitIndicator2_Out' ,'AmountNetUnsigned' ,'deCreditDebitIndicator3_Out' ,'AmtTranDrInReconCurr' ,'deCreditDebitIndicator4_Out' ,'AmtTranCrInReconCurr' ,'deCreditDebitIndicator5_Out' ,'deCreditDebitIndicator6_Out' ,'deCreditDebitIndicator7_Out' ,'AmtNetInReconCurr' ,'deCreditDebitIndicator8_Out' ,'AmtNetFeeInReconCurr' ,'deCreditDebitIndicator9_Out' ,'AmountNetTotalUnsigned' ,'deCreditDebitIndicator10_Out' ,'DebitsTransactionNumber' ,'CreditsTransactionNumber' ,'PDS0397' ,'PDS0398' ,'PDS0399' ,'PAN_Hash' ,'CardNumber4Digits' ,'BINNumber' ,'ChargeAssmtFee' ,'LoyaltyFlag' ,'AcquirerRefNumber' ,'TokenUniqueReference' ,'PANUniqueReference' ,'PaymentHash' ,'ATMStateProvCode' ,'CallOriginStateCode' ,'ProgramParticipationIndicator' ,'IPMOrgTerminalType' ,'IPM_POSCardDataInputMode' ,'IPMCrossBorderIndicator' ,'CPSEnvironment' ,'DigitalWalletIntrchngOverrideIndic0158' ,'DigitalWalletIntrchngOverrideIndic0358' ,'DomesticMerchantTaxID_0221' ,'FinancialAcctInfo_0068' ,\
                                                        'BS_SystemStatus' ,'TranIdOriginal' ,'PODId_FileReceived' ,'PANHash64' ,'IPM_AlgorithmID' ,'Outstg_Status' ,'CardAcceptorIdCode_upd' ,'AcquiringInsitutionIDCode_upd' ,'TransactionDescription' ,'Reversed' ,'creditplanmaster' ,'MessageIndicator' ,'CardDataEntryMode' ,'TxnCreated' ,'CardholderBillingAmountOrg' ,'CurrCodeCardHolderBillingOrg' ,'DateLocalTransactionOrg' ,'TimeLocalTransactionOrg' ,'JulianProSubDateOrg' ,'ExpirationDateOrg' ,'CardDataEntryModeOrg' ,'MessageReasonCodeOrg' ,'BinNumberPDS0001' ,'IsPartialClr' , 'IPMCardPresent', 'IsCreditTxn' ,'CardExpirationDate' ,'TLID']
                            
                            IsAgingReq = 1 if EnvVariable_Dict.get('IsAgingReq',False) else 0
                            Temp_Lst_Value = []
                            Lst_Value = []
                            Addenda_Lst_Value = []
                            Parse_rec_count = 0
                            count = 0
                            TranId = 0
                            Insert_Limit_Value      = EnvVariable_Dict.get('TxnInsertToDB')
                                                        
                            try:
                                with open(InputFilePath, 'rb') as blocked_in:
                                    reader = IpmReader(blocked_in, blocked = EnvVariable_Dict.get('IsBlockedFile'))
                                    for raw_record in reader:
                                        Parse_rec_count = Parse_rec_count + 1
                                        if Parse_rec_count % 100000 == 0:
                                            print(f"reading record {Parse_rec_count}")
                                        Temp_Lst_Value = []
                                        count = Parse_rec_count
                                        
                                        if raw_record.get('MTI') == '1644' and raw_record.get('DE24') == '697':
                                            logger.info("Header 9824 Info: Start Header InsertToDB")
                                            InsQuery = FP_Values.Get_Header(raw_record, InFileName, Inp_Jobid)
                                            SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                                            logger.info("Header 9824 Info: Header stored successfully in database")

                                        elif raw_record.get('MTI') == '1644' and raw_record.get('DE24') == '695':
                                            logger.info("Trailer 9824 Info: Start Trailer InsertToDB")
                                            LastParseMsgNum = int(raw_record.get('DE71'))
                                            InsQuery = FP_Values.Get_Trailer(raw_record, Inp_Jobid)
                                            SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                                            logger.info("Trailer 9824 Info: Trailer stored successfully in database")
                                        
                                        elif (raw_record.get('MTI') in ['1240','1442','1740']) or (raw_record.get('MTI') == '1644' and raw_record.get('DE24') in ['605','640','680','681','685','688','691','693','699']):
                                            TranId += 1
                                            raw_record.update({'TranId' : TranId, 'JobId' : Inp_Jobid})
                                            record = FP_Values.Get_SubFields(raw_record)
                                            
                                            for Column in Interim_Header:
                                                Temp_Lst_Value.append(record.setdefault(Column,None))
                                                
                                            Lst_Value.append(Temp_Lst_Value)
                                            
                                        elif raw_record.get('MTI') == '1644' and raw_record.get('DE24') == '696':
                                            print('vishnu')
                                            print(raw_record)
                                            raw_record.update({'TranId' : TranId, 'JobId' : Inp_Jobid})
                                            record = FP_Values.Get_Addenda(raw_record,Inp_Jobid)
                                            
                                            for Column in Addenda_Buffer_Header:
                                                Temp_Lst_Value.append(record.setdefault(Column,None))
                                                
                                            Addenda_Lst_Value.append(Temp_Lst_Value)
                                                
                                        if len(Lst_Value) >= Insert_Limit_Value:
                                            MutliProcess.insert_to_Sql(Connection_String, Lst_Value, Interim_Header, 'IPMMaster_Interim')
                                            Lst_Value = []
                                            
                                        if len(Addenda_Lst_Value) >= Insert_Limit_Value:
                                            print(Addenda_Lst_Value)
                                            MutliProcess.insert_to_Sql(Connection_String, Addenda_Lst_Value, Addenda_Buffer_Header, 'IPMAddendumDetails_Buffer')
                                            Addenda_Lst_Value = []

                                Insert_Limit_Value = 0
                                if len(Lst_Value) > Insert_Limit_Value:
                                    MutliProcess.insert_to_Sql(Connection_String, Lst_Value, Interim_Header, 'IPMMaster_Interim')
                                    Lst_Value = []
                                    
                                if len(Addenda_Lst_Value) >= Insert_Limit_Value:
                                    MutliProcess.insert_to_Sql(Connection_String, Addenda_Lst_Value, Addenda_Buffer_Header, 'IPMAddendumDetails_Buffer')
                                    Addenda_Lst_Value = []

                                if Parse_rec_count == LastParseMsgNum :
                                    IPM_Select_And_Updates.IPM_Update(3, Connection_String, Inp_Jobid, From = LastParseMsgNum, ArgVar_1 = Inp_Jobid)
                                else:
                                    ErrorReason = f"PARSING : Total Record in file MessageNumber = {LastParseMsgNum} Mismatch with Total Read Records = {Parse_rec_count}"
                                    logger.debug(ErrorReason)
                                    IPM_Select_And_Updates.IPM_Update(1,'ERROR')
                                    Functions.File_Movement(OutFilePath,EnvVariable_Dict.get('OutputDir'))
                                    sys.exit()
                                    
                            except Exception as e:
                                ErrorReason = f"Error Raised PARSING : {e}"
                                ErrorReason = ErrorReason.replace("'","")
                                IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
                                logger.debug(f"Error Raised PARSING : {e}",True )
                                logger.log_exception(*sys.exc_info())
                                sys.exit()

                            current,peak = tracemalloc.get_traced_memory()
                            logger.info("PARSING Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                            tracemalloc.clear_traces()
                            
                            res = IPM_SPCall.IPM_SPCall(Connection_String, Inp_Jobid, Parse_rec_count, EnvVariable_Dict.get('IPM_ValidationEnable'), EnvVariable_Dict.get('IPM_UseCCardOrCCard2'),Stage_StartTime, EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'), FileSource, AgingFlag)
                            
                            if res:
                                current,peak = tracemalloc.get_traced_memory()
                                logger.info("SP Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                                tracemalloc.clear_traces()
                                #Mail.SendEmail(1, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                                logger.info(f"******************************* File Processing For Jobid {Inp_Jobid} Completed *******************************", True)
                                
                            else:
                                logger.error('Some Issue occur in IPM_SPCall Function Please Check')
                                sys.exit()
                            
                            #Resetting Some Variables
                            ErrorReason = ""
                            Parse_rec_count = Rec_Count = 0
                            GotoReadFile = False
                    
        else:
            logger.info("No File Found For Processing")
            time.sleep(EnvVariable_Dict['FileRecheckTime'])
            print("DONE")
            print(datetime.datetime.now())
            print(datetime.datetime.now()-st)