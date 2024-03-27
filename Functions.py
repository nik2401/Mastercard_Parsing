try:
    import os
    import sys
    import time
    import shutil
    import hashlib
    import pathlib
    import datetime
    from dateutil.relativedelta import relativedelta
    from unidecode import unidecode
    from Logger import Logger
    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)
    
######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def adddecimal(Amount,Decimal):
    if Amount is None or Amount == '':
        Amount = '0'
    if Decimal == 0:
        New_Amount = f"{Amount[0:]}.00"
    else:
        New_Amount = f"{Amount[0:-(Decimal)]}.{Amount[-(Decimal):]}"
    return New_Amount

######################################################################################################################################################

def DeleteCSvFolder(strFolderPath):
    FolderLocation = pathlib.Path(strFolderPath)
    if FolderLocation.exists ():
        shutil.rmtree(strFolderPath)
    else:
        logger.debug("Folder does not exist, Proceed Further")
    return None

######################################################################################################################################################

def datetimeconvert(date,time):
    temp_date = f"{date[0:4]}-{date[4:6]}-{date[6:]}"
    temp_time = f"{time[0:2]}:{time[2:4]}:{time[4:]}"
    combdatetime = f"{temp_date} {temp_time}"
    
    try:
        datetime.datetime.strptime(combdatetime,'%Y-%m-%d %H:%M:%S')
    except ValueError as exception:
        logger.error(f"Received Date and Time = {combdatetime} for Record Number")
        
    return combdatetime

######################################################################################################################################################

def Conv_Card_Expiriation_Date(card_exp_date):
    if card_exp_date in ['0000','']:
        return None
    #card_exp_date = f"{raw_record[127:129]}{raw_record[125:127]}" if int(raw_record[125:127]) > 12 else raw_record[125:129]
    card_exp_date = f"{card_exp_date[2:4]}{card_exp_date[:2]}" if int(card_exp_date[:2]) > 12 else card_exp_date
    formatted_date_str = f'01/{card_exp_date[:2]}/{card_exp_date[2:]}'
    formatted_date = datetime.datetime.strptime(formatted_date_str, '%d/%m/%y')
    result_date = formatted_date + relativedelta(months=1) - datetime.timedelta(days=1)
    return result_date
######################################################################################################################################################

def get_file_sizes(directory):
    file_sizes = {}
    for root, _, files in os.walk(directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_sizes[file_path] = os.path.getsize(file_path)
    return file_sizes

######################################################################################################################################################

def process_files_size_check(in_directory, Time_Recheck_FileSize, error_dir):
    # Check For Files In ErrorDIr
    if(len(os.listdir(error_dir)) > 0):
        logger.debug(f"{os.listdir(error_dir)} : File/Folder Is Present In ErrorDir Please Resolve It Before Further Processing",True)
        return False
    else:
        consecutive_same_sizes = 0
        previous_file_sizes = get_file_sizes(in_directory)
        
        while consecutive_same_sizes < 3:
            time.sleep(Time_Recheck_FileSize)
            
            current_file_sizes = get_file_sizes(in_directory)
            
            if current_file_sizes == previous_file_sizes:
                consecutive_same_sizes += 1
                logger.info(f"File sizes are the same for {consecutive_same_sizes} consecutive times.",True)
            else:
                consecutive_same_sizes = 0
                logger.info("File sizes have changed. Restarting the iteration.",True)

            previous_file_sizes = current_file_sizes

            if not current_file_sizes:
                logger.info("No files found. Breaking the loop.")
                return False            
        return True
######################################################################################################################################################

def check_file_out_dir(out_dir,INFileName):
    out_file_list = [oname for oname in os.listdir(out_dir) if os.path.isfile(os.path.join(out_dir, oname)) ]
    out_file_list.sort(key=lambda outs: os.path.getmtime(os.path.join(out_dir, outs)))

    for check_out_file in out_file_list:
        if check_out_file == INFileName:
            logger.error("File Already Present In Out Folder")
            return True
    return False

######################################################################################################################################################
            
def File_Movement(file_src, file_dest, processflag = 0):
    try:
        shutil.move(file_src,file_dest)
        logger.debug(f"File is Moved to the {file_dest} from  {file_src}")
        if processflag == 1: sys.exit()
    except Exception as e:
        logger.log_exception(f"Exception occur in File_Movement: {e}")
   
######################################################################################################################################################
            
def change_file_name(current_path, new_name):
    try:
        # Rename the file
        os.rename(current_path, os.path.join(os.path.dirname(current_path), new_name))
        logger.debug(f"File name changed to: {new_name}")
    except Exception as e:
        logger.error(f"Error changing file name: {e}")
        
######################################################################################################################################################
 
def Gen_FileHash(FilePath):
    try:
        hash  = hashlib.sha256()
        byte  = bytearray(128*1024)
        mv = memoryview(byte)
        
        with open(FilePath, 'rb', buffering=0) as f:
            while n := f.readinto(mv):
                hash.update(mv[:n])
                
        return hash.hexdigest()
    except Exception as e:
        logger.log_exception(f"Exception occur in Gen_FileHash: {e}")
######################################################################################################################################################
