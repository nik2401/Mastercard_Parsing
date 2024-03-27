import binascii
from array import array


class BitArray:
    endian = 'big'
    bytes = b''

    def __init__(self, endian='big'):
        self.endian = endian

    def tobytes(self):
        return self.bytes

    def tolist(self):
        swap_bytes = array('B', self.bytes)
        if self.endian == 'little':
            for i, n in enumerate(swap_bytes):
                swap_bytes[i] = int('{:08b}'.format(n)[::-1], 2)
        width = len(self.bytes)*8
        swapped_bytes = swap_bytes.tobytes()
        bit_list = '{bytes:0{width}b}'.format(bytes=int(binascii.hexlify(swapped_bytes), 16), width=width)
        return [bit == '1' for bit in bit_list]

