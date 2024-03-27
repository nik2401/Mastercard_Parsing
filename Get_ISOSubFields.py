import Functions
import datetime
import re
#from PlatformEncryption import PlatformEncryption

def Update_raw_record_Field(raw_record, source_key, target_key):
    if source_key in raw_record: raw_record.update({target_key: raw_record.pop(source_key)})

def Get_PDS_0148_Iteration(Occurance):
    Temp_Dict = {}
    for i in range(0,len(Occurance)):
        if i < 5:
            Temp_Dict.update({f"CurrencyCode{i+1}_148" : Occurance[i][:3], f"CurrencyExponent{i+1}" : Occurance[i][3:]})
        else:
            Temp_Dict.update({f"CurrencyCode{i+1}" : Occurance[i][:3], f"CurrencyExponent{i+1}" : Occurance[i][3:]})
    return Temp_Dict

def Get_PDS_0146_Iteration(Occurance,raw_record):
    Temp_Dict = {}
    for i in range(0,len(Occurance)):
        Temp_Dict.update({f"FeeTypeCodeTxnFee{i+1}" : Occurance[i][:2], f"FeeProcessCodeTxnFee{i+1}" : Occurance[i][2:4], f"FeeSettleIndicator{i+1}" : Occurance[i][4:6]})
        Temp_Dict.update({f"CurrencyCodeFee{i+1}" : Occurance[i][6:9], f"CurrencyCodeFeeRecon{i+1}" : Occurance[i][21:24]})
        
        Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict,raw_record.get(f"CurrencyCodeFee{i+1}"))
        if Temp_Decimal is None: Temp_Decimal = 2
        Temp_Decimal = float(Functions.adddecimal(str(Occurance[i][9:21]), Temp_Decimal))
        Temp_Dict.update({f"AmountFeeTxnFee{i+1}" : Temp_Decimal})
            
        Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict,raw_record.get(f"CurrencyCodeFeeRecon{i+1}"))
        if Temp_Decimal is None: Temp_Decimal = 2
        Temp_Decimal = float(Functions.adddecimal(str(Occurance[i][24:36]), Temp_Decimal))
        Temp_Dict.update({f"AmountFeeRecon{i+1}" : Temp_Decimal})
        
    return Temp_Dict

def Get_DE54_Iteration(Occurance):
    Temp_Dict = {}
    for i in range(0,len(Occurance)):
        if i == 0:
            Temp_Str        = Occurance[i][9:20]
            Temp_Decimal    = float(Functions.adddecimal(Temp_Str, 2))
            Temp_Dict.update({f"AccountType" : Occurance[i][:2], f"AmountType" : Occurance[i][2:4], f"CurrencyCode" : Occurance[i][4:7], f"AmountSign" : Occurance[i][7:8],\
                                f"Amount" : Temp_Decimal})
        else:
            Temp_Str    = Occurance[i][9:20]
            Temp_Float  = float(Functions.adddecimal(Temp_Str, 2))
            Temp_Dict.update({f"AccountType{i}" : Occurance[i][:2], f"AmountType{i}" : Occurance[i][2:4], f"CurrencyCode{i}" : Occurance[i][4:7],\
                                f"AmountSign{i}" : Occurance[i][7:8], f"Amount{i}" : Temp_Float})
        if i == 5:
            break
        
    return Temp_Dict

def Get_PDS0799_Iteration(Occurance):
    Temp_Dict = {}
    for i in range(0,len(Occurance)):
        Temp_Dict.update({f"AuxillaryField1_0{i}PDS" : Occurance[i][:8], f"AuxillaryField2_0{i}PDS" : Occurance[i][8:33]})
        
    return Temp_Dict
    
def GetExponentAndCurrencyCode(Temp_Dict,DE_Currency) -> int:
    Temp_Iteration = 0
    for i in range(0,15):
        #print(b.get(f"CurrencyCode{i+1}_148"))
        Temp_CurrencyCode = Temp_Dict.get(f"CurrencyCode{i+1}_148") if i < 5 else Temp_Dict.get(f"CurrencyCode{i+1}")
        if Temp_CurrencyCode == DE_Currency:
            Temp_Iteration = i+1
            break
        else:
            Temp_Iteration = 0

    Temp_Decimal = Temp_Dict.get(f"CurrencyExponent{Temp_Iteration}") if Temp_Iteration > 0 else None
    Temp_Decimal =  int(Temp_Decimal) if Temp_Decimal is not None else Temp_Decimal
    
    return Temp_Decimal

def julian_to_datetime(julian_date):
    day_of_year = int(julian_date[1:])
    base_year = datetime.datetime.now().year // 10 * 10 + int(julian_date[0])
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if (base_year % 4 == 0 and (base_year % 100 != 0 or base_year % 400 == 0)):
        days_in_month[1] = 29

    for month, days in enumerate(days_in_month):
        if day_of_year <= days:
            break
        day_of_year -= days

    return datetime.datetime(base_year, month + 1, day_of_year)

def Get_SubFields(raw_record):
    #while True:
    for key, value in raw_record.items():
        if value is None:
            raw_record[key] = ''
    ErrorReason = ''
    if 'MTI' in raw_record: raw_record['MessageTypeIdentifier'] = raw_record.pop('MTI')

    
    
    if 'DE48' in raw_record:
        if 'PDS0501' in raw_record:
            Temp_Str = raw_record.get("PDS0501")
            raw_record.update({'UsageCode' : int(Temp_Str[:2]), 'IndustryRecordNumber' : Temp_Str[2:5], 'OccurrenceIndicator' : Temp_Str[5:8],})

    Update_raw_record_Field(raw_record, 'DE24', 'FunctionCode')

    if 'DE2' in raw_record:
        Temp_Str  = raw_record.get("DE2").strip()
        Pan_Hash  = Functions.KMSHash(Temp_Str)
        raw_record.update({'BINNumber' : Temp_Str[0:6], 'PAN_Hash' : Pan_Hash, 'CardNumber4Digits' : Temp_Str[-4:], 'IPMCardPresent' : '0'})
        del raw_record['DE2']
    else:
        raw_record.update({'IPMCardPresent' : '2'})
    
    if 'DE3' in raw_record:
        raw_record.update({'ProcCode' : raw_record.get('DE3')[0:2], 'ProcCodeFromAccType' : raw_record.get('DE3')[2:4], 'ProcCodeToAccType' : raw_record.get('DE3')[4:], 'ProcCodeIPM' : raw_record.get('DE3')[0:2]})
        del raw_record['DE3']
    
    if raw_record.get('MessageTypeIdentifier') in ['1240','1442','1740']:
        
        Temp_List = ['DE4', 'DE49', 'PDS0148']
        if all(key in raw_record for key in Temp_List):
            if len(raw_record.get('PDS0148')) % 4 == 0:
                pass
            else:
                ErrorReason = f'PDS0148 is not of valid length {len(raw_record.get("PDS0148"))} value = {raw_record.get("PDS0148")} with MessageNumber {raw_record.get("DE71")}'
        else:
            ErrorReason += " ".join(f"{key} is not present in record with MessageNumber {raw_record.get('DE71')}" for key in Temp_List if key not in raw_record)
            
        if 'DE5' in raw_record and 'DE50' not in raw_record:
            ErrorReason = f"DE50 = {raw_record.get('DE50')} is not present in record with MessageNumber {raw_record.get('DE71')}"
            
        if 'DE6' in raw_record and 'DE51' not in raw_record:
            ErrorReason = f"DE51 = {raw_record.get('DE51')} is not present in record with MessageNumber {raw_record.get('DE71')}"
            
        Occurance = [raw_record.get('PDS0148')[i:i+4] for i in range(0, len(raw_record.get('PDS0148')), 4)]
        Temp_Dict_PDS0148 = Temp_Dict = Get_PDS_0148_Iteration(Occurance)
        raw_record = {**raw_record, **Temp_Dict}
        Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict,raw_record.get('DE49'))
        
        if Temp_Decimal is not None:
            TransactionAmount = float(Functions.adddecimal(str(raw_record.get('DE4')), Temp_Decimal))
            
            if raw_record.get('DE24') == '200':
                raw_record.update({'TransactionAmount' : TransactionAmount,'TransactionCurrencyCode' : raw_record.get('DE49')})
            elif raw_record.get('DE24') in ['205','282']:
                raw_record.update({'TxnSrcAmt' : TransactionAmount, 'TransactionAmount' : TransactionAmount, 'TransactionCurrencyCode' : raw_record.get('DE49')})
            else:
                raw_record.update({'TxnSrcAmt' : TransactionAmount, 'TransactionAmount' : TransactionAmount, 'TransactionCurrencyCode' : raw_record.get('DE49')})
        else:
            ErrorReason = f"DE49 = {raw_record.get('DE49')} doesnot match with currency code present in PDS0148 = {raw_record.get('PDS0148')}"

        if 'DE6' in raw_record and 'DE51' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict,raw_record.get('DE51'))
            if Temp_Decimal is not None:
                Temp_Float = float(Functions.adddecimal(str(raw_record.get('DE6')), Temp_Decimal))
                
                if raw_record.get('DE24') == '200':
                    raw_record.update({'CardholderBillingAmount' : Temp_Float, 'CurrCodeCardHolderBilling' : raw_record.get('DE51')})
                elif raw_record.get('DE24') in ['205','282']:
                    raw_record.update({'CardholderBillingAmount' : Temp_Float, 'CardholderBillingAmountOrg' : Temp_Float, 'CurrCodeCardHolderBilling' : raw_record.get('DE51')})
                else:
                    raw_record.update({'CardholderBillingAmount' : Temp_Float, 'CardholderBillingAmountOrg' : Temp_Float, 'CurrCodeCardHolderBilling' : raw_record.get('DE51')})
            else:
                ErrorReason = f"DE51 = {raw_record.get('DE51')} doesnot match with currency code present in PDS0148 = {raw_record.get('PDS0148')} with MessageNumber {raw_record.get('DE71')}"
        else:
            if raw_record.get('DE24') == '200':
                raw_record.update({'CardholderBillingAmount' : TransactionAmount, 'CurrCodeCardHolderBilling' : raw_record.get('DE49'), 'TxnSrcAmt' : TransactionAmount})

        if 'DE5' in raw_record and 'DE50' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict,raw_record.get('DE50'))
            if Temp_Decimal is not None:
                Temp_Float = float(Functions.adddecimal(str(raw_record.get('DE5')), Temp_Decimal))
                
                if raw_record.get('DE24') == '200':
                    raw_record.update({'SettlementAmount' : Temp_Float, 'SettlementCurrencyCode' : raw_record.get('DE50')})
                    raw_record.update({'TxnSrcAmt': Temp_Float}) if 'TxnSrcAmt' not in raw_record else raw_record.__setitem__('TxnSrcAmt', Temp_Float)
                else:
                    raw_record.update({'SettlementAmount' : Temp_Float, 'SettlementCurrencyCode' : raw_record.get('DE50')})
            else:
                ErrorReason = f"DE50 = {raw_record.get('DE50')} doesnot match with currency code present in PDS0148 = {raw_record.get('PDS0148')} with MessageNumber {raw_record.get('DE71')}"
        else:
            if raw_record.get('DE24') == '200':
                raw_record.update({'TxnSrcAmt': 0, 'SettlementAmount' : 0}) if 'TxnSrcAmt' not in raw_record else raw_record.__setitem__('TxnSrcAmt', 0)
             
    Update_raw_record_Field(raw_record, 'DE49', 'TransactionCurrencyCode')
    
    Update_raw_record_Field(raw_record, 'DE50', 'SettlementCurrencyCode')
    
    Update_raw_record_Field(raw_record, 'DE51', 'CurrCodeCardHolderBilling')
    
    [raw_record.pop(key,None) for key in ['DE4','DE5','DE6','DE49','DE50','DE51','PDS0148']]
    
    if 'DE9' in raw_record:
        Temp_Int = int(raw_record.get('DE9')[0])
        Temp_Str = raw_record.get('DE9')[1:]
        Temp_Float = float(Temp_Str) / (10 ** Temp_Int)
        raw_record.update({'ConversionRateSettlement' : Temp_Float})
        
    if 'DE10' in raw_record:
        Temp_Int = int(raw_record.get('DE10')[0])
        Temp_Str = raw_record.get('DE10')[1:]
        Temp_Float = float(Temp_Str) / (10 ** Temp_Int)
        raw_record.update({'ConvRateCardholderBilling' : Temp_Float})
    
    if 'DE12' in raw_record:
        raw_record.update({'TimeLocalTransaction' : raw_record.get('DE12'), 'TransmissionDateTime' : raw_record.get('DE12'), 'DateLocalTransaction' : datetime.datetime.combine(raw_record.get('DE12').date(), datetime.time(0, 0, 0)),\
                            'DateLocalTransactionOrg' : raw_record.get('DE12').strftime('%Y%m%d')[2:], 'TimeLocalTransactionOrg' : raw_record.get('DE12').strftime('%H%M%S')})
        
        del raw_record['DE12']
        
    if 'DE14' in raw_record:
        Temp_Str = raw_record.get('DE14')
        raw_record['ExpirationDateOrg'] = raw_record.pop('DE14')
        #raw_record.update({'CardExpirationDate' : raw_record.get('Temp_Str')})
        
    if 'DE22' in raw_record:
        Temp_Str = raw_record.get('DE22')
        Temp_Dict = {'0' : '00', '1' : '01', '2' : '02', '6' : '79', '7' : '10', 'C' : '05', 'M' : '07', 'N' : '08', 'R' : '09', 'S' : '81', 'T' : '82', 'B': '90', 'A' : '91', 'F' : '05'}
        CardDataEntryMode = Temp_Dict.setdefault(Temp_Str[6],None)
        raw_record.update({'POSInputCap' : Temp_Str[0], 'POSCardHolderAuthCap' : Temp_Str[1], 'POSCardCaptureCapabilities' : Temp_Str[2], 'POSTermOpEnv' : Temp_Str[3],\
                           'POSCardholderPresenceInd' : Temp_Str[4], 'POSCardPresenceIndicator' : Temp_Str[5], 'POSCardDataInputMode' : Temp_Str[6], 'CardDataEntryModeOrg' : Temp_Str[6],\
                           'CardDataEntryMode' : CardDataEntryMode, 'POSCardmemberAuth' : Temp_Str[7], 'POSCardmemberAuthEntity' : Temp_Str[8], 'POSCardDataOutputCapability' : Temp_Str[9],\
                           'POSTerminalOutputCapability' : Temp_Str[10], 'PINEntryCapability' : Temp_Str[11] })
        del raw_record['DE22']
    
    Update_raw_record_Field(raw_record, 'DE23', 'CardSequenceNumber')
    
    Update_raw_record_Field(raw_record, 'DE24', 'FunctionCode')
            
    if 'DE25' in raw_record:
        raw_record['MessageReasonCode'] = raw_record.pop('DE25')
        Temp_Int = 0 if raw_record.get('MessageReasonCode') == '1404' else 1
        raw_record.update({'IsPartialClr' : Temp_Int})
    else:
        raw_record.update({'MessageReasonCode' : '0000', 'IsPartialClr' : 1})
        
    Update_raw_record_Field(raw_record, 'DE26', 'MerchantType')

    if 'DE30' in raw_record:
        Temp_Str = raw_record.get("PDS0149",'')
        Temp_Decimal = '2' if raw_record.get('MessageTypeIdentifier') == '1442' else GetExponentAndCurrencyCode(Temp_Dict_PDS0148,Temp_Str[:3])
            
        if Temp_Decimal is not None:
            Temp_Decimal = float(Functions.adddecimal(raw_record.get('DE30')[:12], Temp_Decimal))
            raw_record.update({'AmountOrgTran' : Temp_Decimal})      
        else:
            ErrorReason = f"PDS0149 = {raw_record.get('PDS0149')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} with MessageNumber {raw_record.get('DE71')}"
        
        if raw_record.get('MessageTypeIdentifier') == '1740':            
            raw_record.update({'TxnSrcAmt': Temp_Decimal}) if 'TxnSrcAmt' not in raw_record else raw_record.__setitem__('TxnSrcAmt', Temp_Decimal)
            raw_record.update({'TransactionCurrencyCode': Temp_Str[:3]}) if 'TransactionCurrencyCode' not in raw_record else raw_record.__setitem__('TransactionCurrencyCode', Temp_Str[:3])
            
        Temp_Decimal = '2' if raw_record.get('MessageTypeIdentifier') == '1442' else GetExponentAndCurrencyCode(Temp_Dict_PDS0148,Temp_Str[3:6])
        
        if Temp_Decimal is not None:
            Temp_Decimal = float(Functions.adddecimal(raw_record.get('DE30')[12:], Temp_Decimal))
            raw_record.update({'AmountOrgRecon' : Temp_Decimal})      
        else:
            ErrorReason = f"PDS0149 = {raw_record.get('PDS0149')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} with MessageNumber {raw_record.get('DE71')}"
        
        del raw_record['DE30']
    
    if 'DE31' in raw_record:
        Temp_Str = raw_record.get('DE31')
        raw_record['AcquirerRefNumber'] = raw_record.pop('DE31')
        raw_record.update({'deInterchangeRateIndicator_Out' : Temp_Str[0], 'deInterchangeRateIndicator' : Temp_Str[0], 'AcquirerID' : Temp_Str[1:7], 'JulianProSubDateorg' : Temp_Str[7:11]})
        Temp_Date = julian_to_datetime(Temp_Str[7:11])
        raw_record.update({'JulianProSubDate' : Temp_Date, 'SeqNumAcqDet' : Temp_Str[11:22], 'CheckDigit' : Temp_Str[22]})
    
    if 'DE32' in raw_record:
        Temp_Str = raw_record.get('DE32').strip()
        Temp_Str = str(int(Temp_Str))
        raw_record['AcquiringInsitutionIDCode'] = raw_record.pop('DE32')
        raw_record['AcquiringInsitutionIDCode'] = Temp_Str
    
    Update_raw_record_Field(raw_record, 'DE26', 'MerchantType')
    
    Update_raw_record_Field(raw_record, 'DE27', 'MerchantCity')
    
    Update_raw_record_Field(raw_record, 'DE33', 'ForwardingInstitutionIDCode')
    
    Update_raw_record_Field(raw_record, 'DE37', 'RetrievalReferenceNumber')
    
    Update_raw_record_Field(raw_record, 'DE38', 'ApprovalCode')
    
    Update_raw_record_Field(raw_record, 'DE40', 'ServiceCode')
    
    Update_raw_record_Field(raw_record, 'DE41', 'CardAcceptorTerminalID')
        
    if 'DE42' in raw_record: raw_record.update({'CardAcceptorIdCode': raw_record.pop('DE42').lstrip('0')})
       
    if 'DE43' in raw_record:
        raw_record.update({'MerchantCountry' : raw_record.get('MerchantCountryCode').strip()}) if 'MerchantCountryCode' in raw_record else raw_record.update({'MerchantCountry' : '', 'MerchantCountryCode' : '', 'IPMCrossBorderIndicator' : '1'})
        raw_record.update({'IPMCrossBorderIndicator' : '1'}) if raw_record.get('MerchantCountry') in ['USA','',None] else raw_record.update({'IPMCrossBorderIndicator' : '0'})            
        raw_record.update({'MerchantStateProvinceCode' : raw_record.get('MerchantStProvCode').strip()}) if 'MerchantStProvCode' in raw_record else raw_record.update({'MerchantStateProvinceCode' : '', 'MerchantStProvCode' : ''})
        raw_record.update({'MerchantStreetAddress' : raw_record.get('MerchantStreetAddress').strip()}) if 'MerchantStreetAddress' in raw_record else raw_record.update({'MerchantStreetAddress' : ''})
        raw_record.update({'MerchantCity' : raw_record.get('MerchantCity').strip()}) if 'MerchantCity' in raw_record else raw_record.update({'MerchantCity' : ''})
        raw_record.update({'MerchantLocPostalCode' : raw_record.get('MerchantLocPostalCode').strip()}) if 'MerchantLocPostalCode' in raw_record else raw_record.update({'MerchantLocPostalCode' : ''})
        Temp_Str = f"{Temp_Str[:22]} {raw_record.get('MerchantStreetAddress').strip()[0:48]} {raw_record.get('MerchantCity').strip()[:13]} {raw_record.get('MerchantLocPostalCode').strip()[:10]} {raw_record.get('MerchantStProvCode').strip()[:3]} {raw_record.get('MerchantCountryCode').strip()[:3]}"
        raw_record.update({'CardAcceptorNameLocation' : Temp_Str})
        del raw_record['DE43']
    
    if 'DE48' in raw_record:
        if 'PDS0001' in raw_record:
            pass
            Temp_Str  = raw_record.get("PDS0001").strip()
            Pan_Hash  = Functions.KMSHash(Temp_Str)
            raw_record.update({'BinNumberPDS0001' : Temp_Str[0:6], 'BINNumber' : Temp_Str[0:6], 'PAN_Hash' : Pan_Hash, 'CardNumber4Digits' : Temp_Str[-4:]})
            del raw_record['PDS0001']
        
        Update_raw_record_Field(raw_record, 'PDS0002', 'gcmsproductidentifier')
        
        Update_raw_record_Field(raw_record, 'PDS0003', 'mtMSMCAdditional')
            
        if 'PDS0004' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0004-' + raw_record.get("PDS0004")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0004']
            
        if 'PDS0006' in raw_record:
            raw_record.update({'CardProgramIdentifier' : raw_record.get("PDS0006")[:3], 'BussinessServiceAgreementTypeCode' : raw_record.get("PDS0006")[3:4], 'deBusinessServiceIDCode' : raw_record.get("PDS0006")[4:10]})
            del raw_record['PDS0006']
            
        if 'PDS0014' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0014-' + raw_record.get("PDS0014")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0014'] 
                        
        if 'PDS0015' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str_1 = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0015')[:6]
            Temp_Str = Temp_Str + 'PDS-0015-SF1-' + Temp_Str_1 + ' | PDS-0015-SF2-' + raw_record.get('PDS0025')[6:7]
            del raw_record['PDS0015']
            
        if 'PDS0018' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0018-' + raw_record.get("PDS0018")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0018']
            
        if 'PDS0021' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0021-' + raw_record.get("PDS0021")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0021']
            
        if 'PDS0023' in raw_record:
            raw_record['TerminalType'] = raw_record.pop('PDS0023')
            raw_record.update({'IPMOrgTerminalType' : raw_record.get('TerminalType').strip(), 'OriginalTerminalType' : raw_record.get('TerminalType').strip()})
            
        if 'PDS0025' in raw_record:
            try:
                Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0025').strip()[1:]
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'CentSiteProcDateOMess' : Temp_Date, 'CentSiteProcDateOFile' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0025 subField 2 {raw_record.get('PDS0025')} with MessageNumber {raw_record.get('DE71')}"
                
            raw_record.update({'MessageReversalIndicator' : raw_record.get('PDS0025').strip()[:1], 'MessageReversalInd' : raw_record.get('PDS0025').strip()[:1],\
                                'CentSiteProcDateOFile' : Temp_Date, 'CentSiteProcDateOMess' : Temp_Date})
            del raw_record['PDS0025']
        else:
            raw_record.update({'MessageReversalInd' : '!','MessageReversalIndicator' : ''})
            
        if 'PDS0026' in raw_record:
            try:
                Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0026').strip()[1:]
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0026 subField 2 {raw_record.get('PDS0026')} with MessageNumber {raw_record.get('DE71')}"
                
            raw_record.update({'FileReversalIndicator' : raw_record.get('PDS0026').strip()[:1], 'CentSiteProcDateOFile' : Temp_Date})
            del raw_record['PDS0026']
            
        if 'PDS0028' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0028-' + raw_record.get("PDS0028")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0028']
            
        if 'PDS0029' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0029-' + raw_record.get("PDS0029")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0029']
        
        Update_raw_record_Field(raw_record, 'PDS0043', 'ProgramRegistrationID')
            
        if 'PDS0044' in raw_record:
            raw_record.update({'ProgramParticipationIndicator' : raw_record.get('PDS0044'), 'FutureUse1_PDS0044' : raw_record.get('PDS0044')[:1]})
            del raw_record['PDS0044']
            
        if 'PDS0052' in raw_record:
            Temp_Str = raw_record.get('PDS0052')
            raw_record.update({'SecurityProtocol' : Temp_Str[:1], 'CardHolderAuthentication' : Temp_Str[1:2], 'UCAFCollectionIndicator' : Temp_Str[2:3]})
            del raw_record['PDS0052']
        
        Update_raw_record_Field(raw_record, 'PDS0056', 'MCElectronicCardIndicator')
        
        Update_raw_record_Field(raw_record, 'PDS0057', 'TxnCategoryIndicator')
        
        Update_raw_record_Field(raw_record, 'PDS0059', 'TokenRequestorID')
        
        Update_raw_record_Field(raw_record, 'PDS0068', 'FinancialAcctInfo_0068')
        
        if 'PDS0080' in raw_record:
            Temp_Str = raw_record.get('PDS0080')
            Temp_Float = float(Functions.adddecimal(str(Temp_Str[3:15]), 2))
            raw_record.update({'TaxAmountRateTypeCode' : Temp_Str[:3], 'TaxAmountValueAddedTax' : Temp_Float, 'TaxAmountCurrencyCode' : Temp_Str[15:18],\
                                'TaxAmountCurrencyExponent' : Temp_Str[18], 'TaxAmountDebitCreditIndicator' : Temp_Str[19]})
            del raw_record['PDS0080']
            
        Update_raw_record_Field(raw_record, 'PDS0137', 'FeeCollectionControlNumber')
        
        Update_raw_record_Field(raw_record, 'PDS0138', 'SourceMessageNumberID')
            
        if 'PDS0145' in raw_record:
            Temp_Str = raw_record.get('PDS0145')
            Temp_Float = float(Functions.adddecimal(str(Temp_Str[3:]), 2))
            raw_record.update({'AlternateTransactionFee_CurrencyCode' : raw_record.get('PDS0145')[:3], 'AlternateTransactionFee_Amount' : Temp_Float})
            
        if 'PDS0146' in raw_record:
            if len(raw_record.get('PDS0146')) % 12 == 0:
                Occurance = [raw_record.get('PDS0146')[i:i+36] for i in range(0, len(raw_record.get('PDS0146')), 36)]
                Temp_Dict = Get_PDS_0146_Iteration(Occurance,raw_record)
                # Use ** to unpack Temp_Dict into raw_record
                raw_record = {**raw_record, **Temp_Dict}
            
            del raw_record['PDS0146']
        
        if 'PDS0149' in raw_record:
            raw_record.update({'CurrencyCodeOriginalTxnAmount' : raw_record.get('PDS0149')[:3], 'CurrencyCodeOrgReconAmount' : raw_record.get('PDS0149')[3:]})
            del raw_record['PDS0149']
            
        Update_raw_record_Field(raw_record, 'PDS0157', 'AlternateProcessorIndicator')
            
        if 'PDS0158' in raw_record:
            raw_record.update({'AcceptanceBrandIDcode' : raw_record.get('PDS0158')[:3], 'TheFinancialNetworkCode' : raw_record.get('PDS0158')[:3], 'BusinessServiceLevelCode' : raw_record.get('PDS0158')[3:4], 'BusinessServiceIDCode' : raw_record.get('PDS0158')[4:10], 'InterchangeRateIndicator' : raw_record.get('PDS0158')[10:12]})
            
            if raw_record.get('PDS0158').strip()[12:18] in ['',None]:
                Temp_Date = None
            else:
                try:
                    Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0158').strip()[12:18]
                    Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                except Exception as e:
                    ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0158 subField 5 {raw_record.get('PDS0158')} with MessageNumber {raw_record.get('DE71')}"
                
            raw_record.update({'BusinessDate' : Temp_Date})
            raw_record.update({'BusinessCycle' : raw_record.get('PDS0158')[18:20], 'mccoverrideindicator' : raw_record.get('PDS0158')[20:21], 'productoverrideindicator' : raw_record.get('PDS0158')[21:24]})
            raw_record.update({'rateapplyindicator' : raw_record.get('PDS0158')[24:25], 'ATMLatePresentmentIndicator' : raw_record.get('PDS0158')[25:26], 'MCAssignedIDOvrrideIndicator' : raw_record.get('PDS0158')[26:27]})
            raw_record.update({'PDS0158FutureUse11' : raw_record.get('PDS0158')[27:28], 'PDS0158FutureUse13' : raw_record.get('PDS0158')[28:29], 'PDS0158FutureUse14' : raw_record.get('PDS0158')[29:30], 'DigitalWalletIntrchngOverrideIndic0158' : raw_record.get('PDS0158')[30:31]})

            del raw_record['PDS0158']
            
        if 'PDS0159' in raw_record:
            
            raw_record.update({'SettlementTransferAgentID' : raw_record.get('PDS0159')[:11], 'SettleTransferAgentAcct' : raw_record.get('PDS0159')[11:39], 'SettlementLevelCode' : raw_record.get('PDS0159')[39:40], 'SettlementServiceIDCode' : raw_record.get('PDS0159')[40:50], 'SettleForeignExchRateClass' : raw_record.get('PDS0159')[50:51]})
            
            try:
                Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0159')[51:57]
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'ReconciliationDate' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0159 subField 6 {raw_record.get('PDS0159')} with MessageNumber {raw_record.get('DE71')}"
                
            raw_record.update({'ReconciliationCycle' : raw_record.get('PDS0159')[57:59],'SettlementCycle' : raw_record.get('PDS0159')[65:67]})
            
            try:
                Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0159')[59:65]
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'SettlementDate' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0159 subField 8 {raw_record.get('PDS0159')} with MessageNumber {raw_record.get('DE71')}"
                
            del raw_record['PDS0159']
                
        if 'PDS0165' in raw_record:
            raw_record.update({'SettlementIndicator' : raw_record.get('PDS0165')[0:1], 'SettlementAgreementInfo' : raw_record.get('PDS0165')[1:]})
            del raw_record['PDS0165']
            
        if 'PDS0170' in raw_record:
            raw_record.update({'CustomerServiceNumber' : raw_record.get('PDS0170')[0:16], 'MerchantTelephoneNumber' : raw_record.get('PDS0170')[16:32], 'AdditionalContactInfo' : raw_record.get('PDS0170')[32:]})
            del raw_record['PDS0170']

        if 'PDS0171' in raw_record:
            raw_record.update({'CharacterSetIndicator' : raw_record.get('PDS0171')[0:3], 'MerchantDescriptionData' : raw_record.get('PDS0171')[3:]})
            del raw_record['PDS0171']
            
        Update_raw_record_Field(raw_record, 'PDS0172', 'SoleProprietorName')

        Update_raw_record_Field(raw_record, 'PDS0173', 'LegalCorporateName')

        Update_raw_record_Field(raw_record, 'PDS0174', 'DUN_Dun_Bradstreet')

        Update_raw_record_Field(raw_record, 'PDS0175', 'CardAcceptorURL')
        
        Update_raw_record_Field(raw_record, 'PDS0176', 'MerchantID')
            
        if 'PDS0177' in raw_record:
            raw_record.update({'CrossBorderIndicator' : raw_record.get('PDS0177')[:1], 'CurrencyIndicator' : raw_record.get('PDS0177')[1:2]})
            del raw_record['PDS0177']
            
        if 'PDS0178' in raw_record:
            raw_record.update({'CharacterSetIndicator2' : raw_record.get('PDS0178')[:3], 'CardAcceptorDataDesc' : raw_record.get('PDS0178')[3:]})
            del raw_record['PDS0178']
            
        if 'PDS0181' in raw_record:
            Temp_Str = raw_record.get('PDS0181')
            Temp_List1 = [2,2,5,12,12,5,12,5,1,12]
            Temp_List = []
            Temp_Int = 0
            for i in Temp_List1:
                Temp_List.append(Temp_Str[Temp_Int:Temp_Int+i])
                Temp_Int += i
                
            raw_record.update({'TypeofInstallments' : re.sub(r'0+(.+)', r'\1', Temp_List[0]), 'NumberofInstallments' : re.sub(r'0+(.+)', r'\1', Temp_List[1]),'InterestRatePDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[2]),\
                                'FirstInstallmentAmountPDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[3]), 'SubsequentInstallmentAmountPDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[4]), 'AnnualPercentRatePDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[5]),\
                                    'InstallmentFeePDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[6]), 'CommissionRatePDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[7]), 'CommissionSignPDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[9]),\
                                        'CommissionAmountPDS0181' : re.sub(r'0+(.+)', r'\1', Temp_List[9])})
            del raw_record['PDS0181']
            
        if 'PDS0184' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0184-' + raw_record.get("PDS0184")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0184']
            
        if 'PDS0185' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0185-' + raw_record.get("PDS0185")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0185']
        
        if 'PDS0186' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0186-' + raw_record.get("PDS0186")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0186']
                
        if 'PDS0189' in raw_record:
            raw_record.update({'FormatNumber' : raw_record.get('PDS0189')[:1], 'PhoneData' : raw_record.get('PDS0189')[1:]})
            del raw_record['PDS0189']
            
        Update_raw_record_Field(raw_record, 'PDS0190', 'PartnerIdCode')
        
        Update_raw_record_Field(raw_record, 'PDS0191', 'OriginatingMessageFormat')
        
        Update_raw_record_Field(raw_record, 'PDS0192', 'PaymentTransactionInitiator')
        
        Update_raw_record_Field(raw_record, 'PDS0194', 'RemotePymtsProgData')
            
        if 'PDS0195' in raw_record:
            raw_record.update({'TNumberofInstallments' : raw_record.get('PDS0195')[:3], 'InstallmentOption' : raw_record.get('PDS0195')[3:5],\
                                'InstallmentNumber' : raw_record.get('PDS0195')[5:8], 'BonusCode' : raw_record.get('PDS0195')[8:9],\
                                    'BonusMonthCode' : raw_record.get('PDS0195')[9:10], 'NoOfPayementPerYr' : raw_record.get('PDS0195')[10:11]})
            
            Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0195')[11:23], 2))
            raw_record.update({'BonusAmount' : Temp_Decimal, 'FirstMonthBonusPayement' : raw_record.get('PDS0195')[23:27]})
            del raw_record['PDS0195']
        
        if 'PDS0196' in raw_record:
            raw_record.update({'MobilePhoneNumber' : raw_record.get('PDS0196')[:17], 'MPhoneServiceProvider' : raw_record.get('PDS0196')[17:47]})
            del raw_record['PDS0196']
            
        if 'PDS0197' in raw_record:
            Temp_Str = raw_record.get('PDS0197','')
            Temp_Decimal = float(Functions.adddecimal(Temp_Str[:12], 2))
            raw_record.update({'TaxAmount1' : Temp_Decimal})
            
            Temp_Decimal = float(Functions.adddecimal(Temp_Str[12:24], 2))
            raw_record.update({'TaxAmount2' : Temp_Decimal, 'TaxPercentage' : raw_record.get('PDS0197')[24:29]})
            
            Temp_Decimal = float(Functions.adddecimal(Temp_Str[29:41], 2))
            raw_record.update({'TaxBaseAmount' : Temp_Decimal})
            
            Temp_Decimal = float(Functions.adddecimal(Temp_Str[41:53], 2))
            raw_record.update({'TaxAmount3' : Temp_Decimal})
            
            del raw_record['PDS0197']
        
        Update_raw_record_Field(raw_record, 'PDS0202', 'PrimaryAcctNbrSyntErr')
        
        Update_raw_record_Field(raw_record, 'PDS0204', 'AmountSyntErr')
        
        if 'PDS0205' in raw_record:
            raw_record.update({'DataElementID' : raw_record.get('PDS0205')[0:5], 'ErrorSeverityCode' : raw_record.get('PDS0205')[5:7],\
                                'ErrorMessageCode' : raw_record.get('PDS0205')[7:11], 'SubfieldID' : raw_record.get('PDS0205')[11:14]})
            del raw_record['PDS0205']
            
        if 'PDS0206' in raw_record:
            raw_record.update({'NbrDaysSinceTranOccurred' : raw_record.get('PDS0206')[0:3]})
            del raw_record['PDS0206']
        
        Update_raw_record_Field(raw_record, 'PDS0207', 'WalletIdentifier')
            
        if 'PDS0210' in raw_record:
            raw_record.update({'TransitTranTypeIndicator' : raw_record.get('PDS0210')[0:2], 'TranspModeIndicator' : raw_record.get('PDS0210')[2:4]})
            del raw_record['PDS0210']
        
        Update_raw_record_Field(raw_record, 'PDS0221', 'DomesticMerchantTaxID_0221')
        
        Update_raw_record_Field(raw_record, 'PDS0225', 'ConvertedToAccountNumber')
        
        Update_raw_record_Field(raw_record, 'PDS0228', 'RetrievalDocumentCode')
        
        Update_raw_record_Field(raw_record, 'PDS0240', 'MCPreferedAcqrEndPoint')
        
        Update_raw_record_Field(raw_record, 'PDS0241', 'MCControlNo')
            
        if 'PDS0243' in raw_record:
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0243')[:6]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'MCIssRetrievalReqDate' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0243 subField 1 {raw_record.get('PDS0243')} with MessageNumber {raw_record.get('DE71')}"
                
            raw_record.update({'MCAcqRetrievalRespCode' : raw_record.get('PDS0243')[6:7], 'MCIssuerResponseCode' : raw_record.get('PDS0243')[13:15],\
                                'MCIssuerRejectReasons' : raw_record.get('PDS0243')[21:31], 'MCImageReviewDecision' : raw_record.get('PDS0243')[31:32]})
            
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0243')[7:13]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'MCAcqRetrievalRespSentDate' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0243 subField 3 {raw_record.get('PDS0243')} with MessageNumber {raw_record.get('DE71')}"
                
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0243')[15:21]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'MCIssuerResponseDate' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0243 subField 5 {raw_record.get('PDS0243')} with MessageNumber {raw_record.get('DE71')}"
                
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0243')[32:38]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'MCImageReviewDate' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0243 subField 8 {raw_record.get('PDS0243')} with MessageNumber {raw_record.get('DE71')}"
                
            del raw_record['PDS0243']
            
        if 'PDS0244' in raw_record:
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0244')[:6]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'MCCbackSuppDocDate1' : Temp_Date})
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0244 subfield 1 {raw_record.get('PDS0244')} with MessageNumber {raw_record.get('DE71')}"
            del raw_record['PDS0244']
        
        
        Update_raw_record_Field(raw_record, 'PDS0246', 'MasterComSenderMemo')
        
        Update_raw_record_Field(raw_record, 'PDS0247', 'MasterComReceiverMemo')
        
        Update_raw_record_Field(raw_record, 'PDS0249', 'MasterComRecordId')
                    
        if 'PDS0250' in raw_record:
            raw_record.update({'MasterComSenderEndpointNbr' : raw_record.get('PDS0250')[0:7], 'MasterComRecieverEndpointNbr' : raw_record.get('PDS0250')[7:14]})
            del raw_record['PDS0250']
        
        Update_raw_record_Field(raw_record, 'PDS0253', 'MasterComSystemEnhancedData')
        
        Update_raw_record_Field(raw_record, 'PDS0254', 'MasterComMemberEnhancedData')
        
        Update_raw_record_Field(raw_record, 'PDS0255', 'MasterComMessageType')
            
        if 'PDS0260' in raw_record:
            Temp_Str = raw_record.get('PDS0260')
            raw_record.update({'ExclusionRequestCode' : Temp_Str[:2], 'ExclusionResultsCode' : Temp_Str[3:]})
            del raw_record['PDS0260']
        
        Update_raw_record_Field(raw_record, 'PDS0262', 'DocIndicator')
        
        Update_raw_record_Field(raw_record, 'PDS0263', 'InterchangeLifeCycleValidationCode')
            
        if 'PDS0266' in raw_record:
            
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0266')[4:10]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0266 subField 2 {raw_record.get('PDS0266')} with MessageNumber {raw_record.get('DE71')} Exception Generated : {e}"
                
            raw_record.update({'MessageReasonCodeSec' : raw_record.get('PDS0266')[0:4], 'DateFirstReturnBus' : Temp_Date, 'EditExclReasonCode' : raw_record.get('PDS0266')[10:11],\
                                'EditExclResultsCode' : raw_record.get('PDS0266')[11:12], 'CurrCodeFirstReturn' : raw_record.get('PDS0266')[24:27], 'DataRecFirstReturn' : raw_record.get('PDS0266')[27:127]})
            
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148,raw_record.get('CurrCodeFirstReturn'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0266')[12:24], Temp_Decimal))
                raw_record.update({'AmountFirstReturn' : Temp_Decimal})      
            else:
                ErrorReason = f"PDS0266 = {raw_record.get('PDS0266')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} with MessageNumber {raw_record.get('DE71')}"
                
            del raw_record['PDS0266']

        if 'PDS0267' in raw_record:
            
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0267')[4:10]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0267 subField 2 {raw_record.get('PDS0267')} with MessageNumber {raw_record.get('DE71')} Exception Generated : {e}"
                
            raw_record.update({'MessageReasonCodeSec' : raw_record.get('PDS0267')[0:4], 'DateSecondReturnBus' : Temp_Date, 'EditExclReasonCode' : raw_record.get('PDS0267')[10:11],\
                                'EditExclResultsCode' : raw_record.get('PDS0267')[11:12], 'CurrCodeSecondReturn' : raw_record.get('PDS0267')[24:27], 'DataRecFirstReturn' : raw_record.get('PDS0267')[27:127]})
            
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148,raw_record.get('CurrCodeSecondReturn'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0267')[12:24], Temp_Decimal))
                raw_record.update({'AmountSecondReturn' : Temp_Decimal})      
            else:
                ErrorReason = f"PDS0267 = {raw_record.get('PDS0267')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} with MessageNumber {raw_record.get('DE71')}"
                
            del raw_record['PDS0267']
            
        if 'PDS0268' in raw_record:
            Temp_Str = raw_record.get('PDS0268')
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148,Temp_Str[12:15])
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(Temp_Str[:12], Temp_Decimal))
                raw_record.update({'AmountPartialTransaction' : Temp_Decimal,'CurrencyCodePartialTransaction' : Temp_Str[12:15]})
            else:
                ErrorReason = f"PDS0268 = {raw_record.get('PDS0268')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0268']
            
        if 'PDS0300' in raw_record:
            Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0300')[3:9]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
            except Exception as e:
                ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0300 subField 2 {raw_record.get('PDS0300')} with MessageNumber {raw_record.get('DE71')} Exception Generated : {e}"
                
            raw_record.update({'ReconFileType' : raw_record.get('PDS0300')[:3],'ReconFileReferenceDate' : Temp_Date, 'ReconProcessorID' : raw_record.get('PDS0300')[9:20], 'ReconFileSeqNumber' : raw_record.get('PDS0300')[20:25]})
            
            del raw_record['PDS0300']
            
        Update_raw_record_Field(raw_record, 'PDS0302', 'ReconciledMemberActivity')
        
        if 'PDS0370' in raw_record:
            raw_record.update({'BeginingAcctRangeID' : raw_record.get('PDS0370')[:19], 'EndingAcctRangeID' : raw_record.get('PDS0370')[19:38]})
            
        if 'PDS0372' in raw_record:
            raw_record.update({'ReconMsgTypeIdentifier' : raw_record.get('PDS0372')[:4], 'ReconFunctionCode' : raw_record.get('PDS0372')[4:7]})
            
        Update_raw_record_Field(raw_record, 'PDS0374', 'ReconProcessingCode')
        
        Update_raw_record_Field(raw_record, 'PDS0375', 'MemberReconIndicator1')
        
        Update_raw_record_Field(raw_record, 'PDS0378', 'OriginalReversalTotalIndicator')
        
        if 'PDS0380' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148, raw_record.get('TransactionCurrencyCode'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0380')[1:17], Temp_Decimal))
                raw_record.update({'deCreditDebitIndicator1' : raw_record.get('PDS0380')[:1], 'AmountTransaction' : Temp_Decimal})
            else:
                ErrorReason = f"DE49 = {raw_record.get('TransactionCurrencyCode')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} for PDS0380 with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0380']
        
        if 'PDS0381' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148, raw_record.get('TransactionCurrencyCode'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0381')[1:17], Temp_Decimal))
                raw_record.update({'deCreditDebitIndicator2' : raw_record.get('PDS0381')[:1], 'AmtTranInTranCurr' : Temp_Decimal})
            else:
                ErrorReason = f"DE49 = {raw_record.get('TransactionCurrencyCode')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} for PDS0381 with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0381']
        
        if 'PDS0384' in raw_record:
            Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0384')[1:17], 2))
            raw_record.update({'deCreditDebitIndicator3' : raw_record.get('PDS0384')[:1], 'AmountNetUnsigned' : Temp_Decimal})
            del raw_record['PDS0384']

        if 'PDS0390' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148, raw_record.get('SettlementCurrencyCode'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0390')[1:17], Temp_Decimal))
                raw_record.update({'deCreditDebitIndicator4' : raw_record.get('PDS0390')[:1], 'AmtTranDrInReconCurr' : Temp_Decimal})
            else:
                ErrorReason = f"DE49 = {raw_record.get('SettlementCurrencyCode')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} for PDS0390 with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0390']
            
        if 'PDS0391' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148, raw_record.get('SettlementCurrencyCode'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0391')[1:17], Temp_Decimal))
                raw_record.update({'deCreditDebitIndicator5' : raw_record.get('PDS0391')[:1], 'AmtTranCrInReconCurr' : Temp_Decimal})
            else:
                ErrorReason = f"DE49 = {raw_record.get('SettlementCurrencyCode')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} for PDS0391 with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0391']

        if 'PDS0394' in raw_record:
            Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0394')[1:17], 2))
            raw_record.update({'deCreditDebitIndicator8' : raw_record.get('PDS0394')[:1], 'AmtNetInReconCurr' : Temp_Decimal})            
            del raw_record['PDS0394']
                
        if 'PDS0395' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148, raw_record.get('SettlementCurrencyCode'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0395')[1:17], Temp_Decimal))
                raw_record.update({'DebitCreditIndicator9' : raw_record.get('PDS0395')[:1], 'AmtNetFeeInReconCurr' : Temp_Decimal})
            else:
                ErrorReason = f"DE49 = {raw_record.get('SettlementCurrencyCode')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} for PDS0395 with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0395']
                        
        if 'PDS0396' in raw_record:
            Temp_Decimal = GetExponentAndCurrencyCode(Temp_Dict_PDS0148, raw_record.get('SettlementCurrencyCode'))
            
            if Temp_Decimal is not None:
                Temp_Decimal = float(Functions.adddecimal(raw_record.get('PDS0396')[1:17], Temp_Decimal))
                raw_record.update({'deCreditDebitIndicator10' : raw_record.get('PDS0396')[:1], 'AmountNetUnsigned' : Temp_Decimal})
            else:
                ErrorReason = f"DE49 = {raw_record.get('SettlementCurrencyCode')} doesnot match with currency code present in PDS0148 = {Temp_Dict_PDS0148} for PDS0396 with MessageNumber {raw_record.get('DE71')}"
            
            del raw_record['PDS0396']
                        
        Update_raw_record_Field(raw_record, 'PDS0397', 'PDS0397')
        
        Update_raw_record_Field(raw_record, 'PDS0398', 'PDS0398')
        
        Update_raw_record_Field(raw_record, 'PDS0399', 'PDS0399')
        
        Update_raw_record_Field(raw_record, 'PDS0400', 'DebitsTransactionNumber')
        
        Update_raw_record_Field(raw_record, 'PDS0401', 'CreditsTransactionNumber')
        
        Update_raw_record_Field(raw_record, 'PDS0402', 'TotalTransactionNumber')
        
        Update_raw_record_Field(raw_record, 'PDS0446', 'TranFeeAmtSyntErr')        
            
        if 'PDS0674' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0674-' + raw_record.get("PDS0674")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0674']        
            
        if 'PDS0715' in raw_record:
            Temp_Str = raw_record.setdefault('PDSData', '') + '|' if raw_record.get('PDSData', '') else ''
            Temp_Str = Temp_Str + 'PDS-0715-' + raw_record.get("PDS0715")
            raw_record.update({'PDSData' : Temp_Str})
            del raw_record['PDS0715']        
            
        if 'PDS0799' in raw_record:
            if len(raw_record.get('PDS0799')) % 33 == 0:
                Occurance = [raw_record.get('PDS0799')[i:i+33] for i in range(0, len(raw_record.get('PDS0799')), 33)]
                Temp_Dict = Get_PDS0799_Iteration(Occurance)
                raw_record = {**raw_record, **Temp_Dict}
                del raw_record['PDS0799']
            else:
                ErrorReason = f'PDS0799 is not of valid length {len(raw_record.get("PDS0799"))} value = {raw_record.get("PDS0799")} with MessageNumber {raw_record.get("DE71")}'
        
        if 'PDS1000' in raw_record:
            raw_record['MemberToMemberProprietary_PDS1000'] = raw_record.pop('PDS1000')
    
    if 'DE54' in raw_record:
        if len(raw_record.get('DE54')) % 20 == 0:
            Occurance = [raw_record.get('DE54')[i:i+20] for i in range(0, len(raw_record.get('DE54')), 20)]
            Temp_Dict = Get_DE54_Iteration(Occurance)
            # Use ** to unpack Temp_Dict into raw_record
            raw_record = {**raw_record, **Temp_Dict}
            del raw_record['DE54']
        else:
            ErrorReason = f'DE54 is not of valid length {len(raw_record.get("DE54"))} value = {raw_record.get("DE54")} with MessageNumber {raw_record.get("DE71")}'
    
    Update_raw_record_Field(raw_record, 'DE55', 'ICCSysRelatedData')
    
    if 'DE63' in raw_record:
        Temp_Str = raw_record.get('DE63')[1:]
        raw_record.update({'LifeCycleSupportIndicator' : raw_record.get('DE63')[:1], 'TraceID' : Temp_Str.strip(), 'BankNetReferenceNumber' : Temp_Str[:9]})
        
        if Temp_Str[9:13].isnumeric():
            Temp_Str = str(datetime.datetime.now().year) + Temp_Str[9:13]
            try:
                Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
                raw_record.update({'BankNetDate' : Temp_Date})
            except:
                raw_record.update({'BankNetDate' : None})
        else:
            raw_record.update({'BankNetDate' : None})
            
        del raw_record['DE63']
    
    Update_raw_record_Field(raw_record, 'DE71', 'MessageNumber')
    
    Update_raw_record_Field(raw_record, 'DE72', 'RecordData')
    
    Update_raw_record_Field(raw_record, 'DE93', 'ReceivingICANumber')
    
    Update_raw_record_Field(raw_record, 'DE94', 'SendingICANumber')
    
    Update_raw_record_Field(raw_record, 'DE95', 'CardIssuerRefData')
    
    Update_raw_record_Field(raw_record, 'DE100', 'ReceivingInstIDCode')
    
    Update_raw_record_Field(raw_record, 'DE105', 'TLID')
        
    if raw_record.get('ProcCode') in ['20','28','29']:
        if raw_record.get('MessageReversalIndicator') in ['',None,' ']:
            raw_record.update({'IPM_AlgorithmID' : '-1', 'IsCreditTxn' : 1})
        else:
            raw_record.update({'IPM_AlgorithmID' : '1', 'IsCreditTxn' : 0})
    else:
        if raw_record.get('MessageReversalIndicator') in ['',None,' ']:
            raw_record.update({'IPM_AlgorithmID' : '1', 'IsCreditTxn' : 0})
        else:
            raw_record.update({'IPM_AlgorithmID' : '-1', 'IsCreditTxn' : 0})

    return raw_record

def Get_Header(raw_record, IPM_INFileName, Inp_Jobid):
    try:
        Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0105')[3:9]
        Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
    except Exception as e:
        ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0105 subField 2 {raw_record.get('PDS0105')} with MessageNumber {raw_record.get('DE71')}"
        
    InsQuery = "INSERT INTO IPM_IncomingHeader (MessageTypeIdentifier ,FunctionCode ,FileType ,FileReferenceDate ,ProcessorID ,FileSequenceNumber ,ProcessingMode ,MessageNumber ,ClrFileID ,JobID)"
    InsQuery = f"{InsQuery} VALUES( '{raw_record.get('MTI')}','{raw_record.get('DE24')}','{raw_record.get('PDS0105')[:3]}','{Temp_Date}','{raw_record.get('PDS0105')[9:20]}','{raw_record.get('PDS0105')[20:25]}','{raw_record.get('PDS0122')}','{raw_record.get('DE71')}','{IPM_INFileName}',{Inp_Jobid} )"

    return InsQuery

def Get_Trailer(raw_record, Inp_Jobid):
    try:
        Temp_Str = str(datetime.datetime.now().year)[:2] + raw_record.get('PDS0105')[3:9]
        Temp_Date = datetime.datetime.strptime(Temp_Str, '%Y%m%d')
    except Exception as e:
        ErrorReason = f"{ErrorReason} Invalid Date Received in PDS0105 subField 2 {raw_record.get('PDS0105')} with MessageNumber {raw_record.get('DE71')}"
        
    Temp_Float = float(Functions.adddecimal(raw_record.get('PDS0301','0000000000000000'), 2))
    
    InsQuery = "INSERT INTO IPM_IncomingTrailer ( MessageTypeIdentifier ,FunctionCode ,FileType ,FileReferenceDate ,ProcessorID ,FileSequenceNumber ,FileAmountCheckSum ,FileMessageCounts ,MessageNumber ,JobID )"
    InsQuery = f"{InsQuery} VALUES( '{raw_record.get('MTI')}','{raw_record.get('DE24')}','{raw_record.get('PDS0105')[:3]}','{Temp_Date}','{raw_record.get('PDS0105')[9:20]}','{raw_record.get('PDS0105')[20:25]}','{Temp_Float}','{raw_record.get('PDS0306')}','{raw_record.get('DE71')}',{Inp_Jobid} )"
    
    return InsQuery