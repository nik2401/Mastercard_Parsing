import binascii
import datetime
import decimal
import re
import struct
import sys
from config import config
from BitArray import BitArray
from Logger import Logger

logger = Logger()
DEFAULT_ENCODING = 'latin_1'

def dumps(obj: dict, encoding=None, iso_config=None, hex_bitmap=False):
    if not encoding:
        encoding = DEFAULT_ENCODING
    if not iso_config:
        iso_config = config['bit_config']

    output = _dict_to_iso8583(obj, iso_config, encoding, hex_bitmap)
    return output


def loads(b: bytes, encoding=None, iso_config=None, hex_bitmap=False):
    if not encoding:
        encoding = DEFAULT_ENCODING
    if not iso_config:
        iso_config = config['bit_config']

    return _iso8583_to_dict(b, iso_config, encoding, hex_bitmap)


def _iso8583_to_dict(message, bit_config, encoding=DEFAULT_ENCODING, hex_bitmap=False):

    try:
        if hex_bitmap:
            message_length = len(message)-36
            message_type_indicator, bitmap, message_data = struct.unpack(
                "4s32s" + str(message_length) + "s", message)
            binary_bitmap = binascii.unhexlify(bitmap)

        else:
            message_length = len(message)-20
            message_type_indicator, binary_bitmap, message_data = struct.unpack(
                "4s16s" + str(message_length) + "s", message)
    except struct.error as ex:
        logger.debug(f"Failed unpacking bitmap values, binary_context_data = {message}, original_exception = {ex}",True)
        logger.log_exception(*sys.exc_info())
    return_values = dict()

    # add the message type
    try:
        return_values["MTI"] = message_type_indicator.decode(encoding)
    except UnicodeError as ex:
        logger.debug(f"Failed decoding MTI field, binary_context_data = {message}, original_exception = {ex}",True)
        logger.log_exception(*sys.exc_info())
        
    message_pointer = 0
    bitmap_list = _get_bitmap_list(binary_bitmap)

    for bit in range(2, 128):
        if bitmap_list[bit]:
            logger.debug(f"processing bit {bit}")
            return_message, message_increment = _iso8583_to_field(
                bit,
                bit_config[str(bit)],
                message_data[message_pointer:],
                encoding)

            # Increment the message pointer and process next field
            message_pointer += message_increment
            #print(return_message)
            return_values.update(return_message)

    # check that all of message has been consumed, otherwise raise exception
    if message_pointer != len(message_data):
        logger.log_exception(f"Message data not correct length. Bitmap indicates len={message_pointer}, message is len = {len(message_data)}, binary_context_data = {message}")
    return return_values


def _dict_to_iso8583(message, bit_config, encoding=DEFAULT_ENCODING, hex_bitmap=False):
    output_data = b''
    bitmap_values = [False] * 128
    bitmap_values[0] = True  # set bit 1 on for presence of bitmap

    # get the pds fields from config
    de_pds_fields = sorted(
        [int(key) for key in bit_config if bit_config[key].get('field_processor') == 'PDS'], reverse=True)
    logger.debug(de_pds_fields)

    for de_field_value in _pds_to_de(message):
        de_field_key = de_pds_fields.pop()
        logger.debug(f'de{de_field_key}={de_field_value}')
        message[f'DE{de_field_key}'] = de_field_value

    for bit in range(2, 128):
        if message.get('DE' + str(bit)) or message.get('DE' + str(bit)) == 0:  # 0 evals to false, allow zero values
            logger.debug(f'processing bit {bit}')
            bitmap_values[bit - 1] = True
            logger.debug(message.get('DE' + str(bit)))
            output_data += _field_to_iso8583(
                bit_config[str(bit)],
                message.get('DE' + str(bit)),
                encoding=encoding)

    if hex_bitmap:
        bitmap = binascii.hexlify(bytes(bitmap_values))
    else:
        bitmap = bytes(bitmap_values)

    mti = message['MTI'].encode(encoding) if message.get('MTI') else b''
    output_string = mti + bitmap + output_data
    return output_string


def _field_to_iso8583(bit_config, field_value, encoding=DEFAULT_ENCODING):

    output = ''
    field_value = _pytype_to_string(field_value, bit_config)
    field_length = bit_config.get('field_length')
    length_size = _get_field_length(bit_config)  # size of length for llvar and lllvar fields
    if length_size > 0:
        field_length = len(field_value)
        output += format(field_length, '0' + str(length_size))
    output += format(field_value[:field_length], '<' + str(field_length))
    return output.encode(encoding)


def _iso8583_to_field(bit, bit_config, message_data, encoding=DEFAULT_ENCODING):
    field_length = bit_config['field_length']

    length_size = _get_field_length(bit_config)

    if length_size > 0:
        field_length_string = message_data[:length_size]
        try:
            field_length_string = field_length_string.decode(encoding)
        except UnicodeDecodeError as ex:
            logger.debug(f"Unable to decode DE {bit} field length, binary_context_data = {message_data}, original_exception = {ex}",True)
            logger.log_exception(*sys.exc_info())

        try:
            field_length = int(field_length_string)
        except ValueError as ex:
            logger.debug(f"Invalid field length DE{bit}, binary_context_data = {message_data}, original_exception = {ex}",True)
            logger.log_exception(*sys.exc_info())

    field_data = message_data[length_size:length_size + field_length]
    logger.debug(f'field_data={field_data}')
    field_processor = bit_config.get('field_processor')

    # do ascii conversion except for ICC field
    if field_processor != 'ICC':
        try:
            field_data = field_data.decode(encoding)
        except UnicodeDecodeError as ex:
            logger.debug(f"Unable to decode DE{bit} field value,  binary_context_data = {message_data}, original_exception = {ex}",True)
            logger.log_exception(*sys.exc_info())
    # if field is PAN type, mask the card value
    if field_processor == 'PAN-PREFIX':
        field_data = _pan_prefix(field_data)

    # do field conversion to native python type
    try:
        field_data = _string_to_pytype(field_data, bit_config)
    except ValueError as ex:
        logger.debug(f"Unable to convert DE{bit} field to python type,  binary_context_data = {message_data}, original_exception = {ex}",True)
        logger.log_exception(*sys.exc_info())
    return_values = dict()

    # add value to return dictionary
    return_values["DE" + str(bit)] = field_data

    # if a PDS field, break it down again and add to results
    if field_processor == 'PDS':
        return_values.update(_pds_to_dict(field_data))

    # if a DE43 field, break in down again and add to results
    if field_processor == 'DE43':
        processor_config = bit_config.get('field_processor_config')
        return_values.update(_get_de43_fields(field_data, processor_config))

    # if ICC field, break into tags
    if field_processor == 'ICC':
        return_values.update(_icc_to_dict(field_data))

    return return_values, field_length + length_size


def _pan_prefix(field_data):
    return field_data[:9]


def _string_to_pytype(field_data, bit_config):
    field_python_type = bit_config.get('field_python_type')
    field_date_format = bit_config.get('field_date_format', "%y%m%d")

    if field_python_type in ("int", "long"):
        field_data = int(field_data)
    if field_python_type == "decimal":
        field_data = decimal.Decimal(field_data)
    if field_python_type == "datetime":
        field_data = datetime.datetime.strptime(field_data, field_date_format)
    return field_data


def _pytype_to_string(field_data, bit_config):
    field_python_type = bit_config.get('field_python_type')
    field_date_format = bit_config.get('field_date_format', "%y%m%d")

    return_string = field_data
    if field_python_type in ('int', 'long'):
        return_string = format(int(field_data), '0' + str(bit_config.get('field_length', 0)) + 'd')
    if field_python_type == "decimal":
        return_string = format(decimal.Decimal(field_data), '0' + str(bit_config.get('field_length', 0)) + 'f')
    if field_python_type == "datetime":
        if not isinstance(field_data, datetime.datetime):
            field_data = _get_date_from_string(field_data)
        return_string = format(field_data, field_date_format)
    return return_string


def _get_date_from_string(field_data: str) -> datetime:
    try:
        import dateutil.parser as parser
        print('Using dateutil parser')
        return parser.parse(field_data)
    except ImportError:
        pass

    if sys.version_info >= (3, 7):
        print('Using fromisoformat')
        return datetime.datetime.fromisoformat(field_data)

    # fallback parser -- tries a few different formats until one works
    print('Using built in date parser')
    date_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
    output_date = None
    for date_format in date_formats:
        try:
            output_date = datetime.datetime.strptime(field_data, date_format)
            break
        except ValueError:
            continue
    if not output_date:
        raise ValueError("Unrecognised date string format - {}".format(field_data))
    return output_date


def _get_field_length(bit_config):
    length_size = 0

    if bit_config['field_type'] == "LLVAR":
        length_size = 2
    elif bit_config['field_type'] == "LLLVAR":
        length_size = 3

    return length_size


def _get_bitmap_list(binary_bitmap):
    working_bitmap_list = BitArray(endian='big')
    working_bitmap_list.frombytes(binary_bitmap)

    # Add bit 0 -> original binary bitmap
    bitmap_list = [binary_bitmap]

    # add bits from bitmap
    bitmap_list.extend(working_bitmap_list.tolist())

    return bitmap_list


def _pds_to_de(dict_values):
    # get the PDS field keys in order
    logger.debug(f'dict_values={dict_values}')
    keys = sorted([key for key in dict_values if key.startswith('PDS')])
    logger.debug(f'keys={keys}')
    output = ''
    outputs = []
    for key in keys:
        tag = int(key[3:])
        logger.debug(f'tag={tag}')
        length = len(dict_values[key])
        add_output = f'{tag:04}{length:03}{dict_values[key]}'
        if len(output + add_output) > 999:
            outputs.append(output)
            output = ''
        output += add_output
    if output:
        outputs.append(output)
    logger.debug(f'>pds2de: {outputs}')

    return outputs


def _pds_to_dict(field_data):
    field_pointer = 0
    return_values = {}

    while field_pointer < len(field_data):
        # get the pds tag id
        pds_field_tag = field_data[field_pointer:field_pointer+4]
        logger.debug(f"pds_field_tag = {pds_field_tag}")

        # get the pds length
        pds_field_length = int(field_data[field_pointer+4:field_pointer+7])
        logger.debug(f"pds_field_length = {pds_field_length}")

        # get the pds data
        pds_field_data = field_data[field_pointer+7:field_pointer+7+pds_field_length]
        logger.debug(f"pds_field_data = {pds_field_data}")
        return_values["PDS" + pds_field_tag] = pds_field_data

        # increment the fieldPointer
        field_pointer += 7+pds_field_length

    return return_values


def _icc_to_dict(field_data):
    TWO_BYTE_TAG_PREFIXES = [b'\x9f', b'\x5f']

    field_pointer = 0
    return_values = {"ICC_DATA": binascii.b2a_hex(field_data).decode()}

    while field_pointer < len(field_data):
        # get the tag id (one byte)
        field_tag = field_data[field_pointer:field_pointer+1]
        # set to 2 bytes if 2 byte tag
        if field_tag in TWO_BYTE_TAG_PREFIXES:
            field_tag = field_data[field_pointer:field_pointer+2]
            field_pointer += 2
        else:
            field_pointer += 1

        field_tag_display = binascii.b2a_hex(field_tag)
        logger.debug(f"field_tag_display = {field_tag_display}")

        # stop processing de55 if low values tag found
        if field_tag_display == b'00':
            break

        field_length_raw = field_data[field_pointer:field_pointer+1]
        field_length = struct.unpack(">B", field_length_raw)[0]

        logger.debug(f" field_tag_display : {field_tag_display}, field_length : {field_length}")

        # get the tag data
        de_field_data = field_data[field_pointer+1:field_pointer+field_length+1]
        de_field_data_display = binascii.b2a_hex(de_field_data).decode()
        logger.debug(f"de_field_data_display = {de_field_data_display}")
        return_values["TAG" + field_tag_display.upper().decode()] = de_field_data_display

        # increment the fieldPointer
        field_pointer += 1+field_length

    return return_values


def _get_de43_fields(de43_field, processor_config=None):
    logger.debug(f"de43_field = {de43_field}")

    # No field config provided, just exit
    if not processor_config:
        return dict()

    # perform regex field matching
    de43_regex = processor_config
    field_match = re.match(de43_regex, de43_field)
    if not field_match:
        return dict()

    # get the dict
    field_dict = field_match.groupdict()
    if field_dict.get('DE43_POSTCODE'):
        field_dict['DE43_POSTCODE'] = field_dict['DE43_POSTCODE'].rstrip()
    return field_dict


