import sys
import struct
import typing
import iso8583
from Logger import Logger

logger = Logger()

class Unblock1014(object):
    def __init__(self, file_obj: typing.BinaryIO):
        self.file_obj = file_obj
        self.buffer = b''

    def __getattr__(self, name: str) -> any:
        try:
            return self.__dict__[name]
        except KeyError:
            if hasattr(self.file_obj, name):
                return getattr(self.file_obj, name)
        return None

    def read(self, bytes_to_read: int = 0):
        read_all = True if not bytes_to_read else False
        while read_all or len(self.buffer) <= bytes_to_read:
            block = self.file_obj.read(1014)
            if not block:  # eof
                break
            self.buffer += block[:1012]
        output = self.buffer[:bytes_to_read]
        self.buffer = self.buffer[bytes_to_read:]
        return output

class VbsReader(object):
    record_number = 1
    last_record = None

    def __init__(self, vbs_file: typing.BinaryIO, blocked: bool = False):
        self.vbs_data = vbs_file
        if blocked:
            self.vbs_data = Unblock1014(vbs_file)

    def __getattr__(self, name) -> any:
        try:
            return self.__dict__[name]
        except KeyError:
            if hasattr(self.vbs_data, name):
                return getattr(self.vbs_data, name)
        return None

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        record_length_raw = self.vbs_data.read(4)
        if len(record_length_raw) != 4:
            # this can happen if the VBS does not have a zero length record at end.
            # You can recreate using VbsWriter and not calling close method.
            # The reader will just accept we are at end if this happens.
            logger.warning(f'Unable to read next record length - requested 4 bytes,'
                           f' got {len(record_length_raw)} -- assuming end of data')
            raise StopIteration

        record_length = struct.unpack(">i", record_length_raw)[0]
        logger.debug(f"record_length= {record_length}")

        # throw mcipm data error if length is negative or excessively large (indicates bad input)
        if record_length < 0 or record_length > 3000:
            logger.error(f"Invalid record length - value read was {record_length}, record_number = {self.record_number}, binary_context_data = {self.last_record}")

        # exit if last record (length=0)
        if record_length == 0:
            raise StopIteration

        record = self.vbs_data.read(record_length)
        if len(record) != record_length:         
            logger.error(f"Unable to read complete record - record length: {record_length}, data read: {len(record)}, record_number = {self.record_number}, binary_context_data = {record_length_raw + record}")

        self.last_record = record_length_raw + record  # save last record read
        self.record_number += 1    # increment record counter
        return record  # get the full record including the record length

class IpmReader(VbsReader):
    def __init__(self, ipm_file: typing.BinaryIO, encoding: str = None, iso_config: dict = None, **kwargs):
        self.encoding = encoding
        self.iso_config = iso_config
        super(IpmReader, self).__init__(ipm_file, **kwargs)

    def __next__(self) -> dict:
        vbs_record = super(IpmReader, self).__next__()
        logger.debug(f'{len(vbs_record)}: {vbs_record}')
        try:
            output = iso8583.loads(vbs_record, encoding=self.encoding, iso_config=self.iso_config)
        except Exception as ex:
            logger.debug(f"Error while loading ISO8583 record, record number = {self.record_number}, binary_context_data = {self.last_record}, original_exception = {ex}",True)
            logger.log_exception(*sys.exc_info())
        return output
