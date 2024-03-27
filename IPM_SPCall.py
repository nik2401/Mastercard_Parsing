try:
    import sys
    #import Mail
    import datetime
    import SQL_Connections
    from Logger import Logger
    import IPM_Select_And_Updates
    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def SP_Call_ChangeFileStatus(Connection_String, IPMFileSequence):
    try:
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_ChangeFileStatusInQueuetoNew '{IPMFileSequence}'")
    except Exception as e:
        ErrorReason = f"Error Raised PR_ChangeFileStatusInQueuetoNew : {e} and result of SP = {SP_result[-1][-1]}"
        logger.debug(ErrorReason, True)
        logger.log_exception(*sys.exc_info())
        sys.exit()
        
    return SP_result[-1][-1]

#############################################################################################################################################

def IPM_SPCall(Connection_String, Inp_Jobid, Parse_rec_count, ValidationEnable, UseCCard, Stage_StartTime, EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, FileSource,AgingFLag):

    pfurther = False
    SP_result = 0
    SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_MCIPM_UpdAdditionalDetails {Inp_Jobid},{Parse_rec_count},{AgingFLag}",Inp_Jobid)
    logger.info(f"SP Result = {SP_result[-1][-1]}")

    pfurther = CheckFileStatus(Connection_String, Inp_Jobid)

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0

        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"PARSING {TimeDiff[:-3]}"
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'UPDATEACCOUNT', 'PARSING', TimeTaken, To = 4, From = 2)
        Stage_StartTime = datetime.datetime.now()
        
        logger.info("************************************ PARSING END ************************************",True)
        
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
        
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_MCIPMUpdateAcctDetails {Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")
        
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_MCIPM_UpdAdditionalDetails Or SP Fail To Execute Successfully"
        ErrorReason = ErrorReason.replace("'","")
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        logger.error(ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"UPDATEACCOUNT {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'AUTHMATCHING','UPDATEACCOUNT', TimeTaken, To = 5, From = 3)
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
                
        ProcessingDateTime = datetime.datetime.now()
        Ins_ProcessingDateTime = ProcessingDateTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_MCIPMAuthMatchingDynamic '{Ins_ProcessingDateTime}',{Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")   
                    
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = f"File Status in ClearingFiles gets ERROR while executing PR_MCIPMUpdateAcctDetails Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        ProcessingDateTime = datetime.datetime.now()
        Ins_ProcessingDateTime = ProcessingDateTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_MCIPMGenerateLCIDForUnMatchedClr {Inp_Jobid},'{Ins_ProcessingDateTime}'",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")

        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_MCIPMAuthMatchingDynamic Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()
    
    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"AUTHMATCHING {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'REVMATCHING','AUTHMATCHING', TimeTaken, To = 6, From = 4)
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_MCIPMRevMatchingDynamic {Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")

        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_MCIPMGenerateLCIDForUnMatchedClr Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"REVMATCHING {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'INVALIDTXN','REVMATCHING', TimeTaken, To = 7, From = 5)
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
        
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_GenerateIPMTxnForInvalidCard {Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")            
            
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_MCIPMRevMatchingDynamic Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"INVALIDTXN {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'SECONDPRESENTMENT','INVALIDTXN', TimeTaken, To = 8, From = 6)
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC pr_IPMSecondPresentment {Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")
            
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_GenerateIPMTxnForInvalidCard Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0

        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"SECONDPRESENTMENT {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'CLEARING','SECONDPRESENTMENT', TimeTaken, To = 9, From = 7)
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_GenerateIPMTransactions {Inp_Jobid},{UseCCard},{ValidationEnable},'{FileSource}'",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")
            
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing pr_IPMSecondPresentment Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = True

        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"CLEARING {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        IPM_Select_And_Updates.IPM_Update(1, Connection_String, Inp_Jobid, 'DONE','CLEARING', TimeTaken, To = 10, From = 8)
        #Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
        
    else:
        pfurther = False
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_GenerateIPMTransactions Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    return pfurther
######################################################################################################################################################

def CheckFileStatus(Connection_String, Inp_Jobid):
    res = IPM_Select_And_Updates.IPM_Select(8,Connection_String, Inp_Jobid, ArgVar_1 = 'MASTERCARDIPM')
    if res[0][0] == 'ERROR':
        return False
    else:
        return True

######################################################################################################################################################