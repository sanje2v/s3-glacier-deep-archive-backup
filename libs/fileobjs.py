import os.path

from Cryptodome.Cipher import ChaCha20

import settings
from utils import repeat_string_until_length, str_to_bytes


class EncryptSplitFileObj:
    def __init__(self,
                 output_filename: str,
                 encrypt_key: bytes | None):
        nonce: str = repeat_string_until_length(os.path.basename(output_filename), settings.ENCRYPT_NONCE_LENGTH)
        self.chacha20 = ChaCha20.new(key=encrypt_key, nonce=str_to_bytes(nonce)) if encrypt_key else None
        self.output_file = open(output_filename, mode='wb')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def tell(self):
        return self.output_file.tell()

    def readable(self):
        return False

    def writable(self):
        return True

    def seekable(self):
        return False

    def write(self, b, /):
        if self.chacha20 is not None:
            b = self.chacha20.encrypt(b)

        self.output_file.write(b)

    def close(self):
        if self.output_file:
            self.output_file.close()
            self.output_file = None


class DecryptFileObj:
    def __init__(self, filename: str, decrypt_key: bytes):
        self.file = open(filename, mode='rb')
        nonce: str = repeat_string_until_length(os.path.basename(filename), settings.ENCRYPT_NONCE_LENGTH)
        self.chacha20 = ChaCha20.new(key=decrypt_key, nonce=str_to_bytes(nonce))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def decrypt(self, output_filename, buffer_size):
        with open(output_filename, mode='wb') as output_file:
            while True:
                data = self.file.read(buffer_size)
                if not data:
                    break

                data = self.chacha20.decrypt(data)
                output_file.write(data)
