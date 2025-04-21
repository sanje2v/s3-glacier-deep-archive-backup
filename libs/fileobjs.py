import os.path
from copy import deepcopy
from Cryptodome.Cipher import ChaCha20


class EncryptSplitFileObj:
    def __init__(self,
                 output_filename_template: str,
                 encrypt_key: bytes,
                 upload_callback: callable):
        self.output_filename_template = output_filename_template
        self.encrypt_key = encrypt_key
        self.upload_callback = upload_callback

        if encrypt_key is not None:
            self.chacha = ChaCha20.new(key=encrypt_key)
        self.idx = -1
        self.last_file = None

        self.create_new_part()

    def _create_output_filename(self, idx: int):
        return os.path.join(os.path.dirname(self.output_filename_template),
                            f"{idx:03}_{os.path.basename(self.output_filename_template)}")

    def _close_last_file(self):
        if self.last_file is not None:
            self.last_file.close()
            self.upload_callback(deepcopy(self.last_file.name))

    def last_part_size(self):
        match self.last_file:
            case None:
                return 0
            case _:
                return self.last_file.tell()

    def create_new_part(self):
        self._close_last_file()

        self.idx += 1
        self.last_file = open(self._create_output_filename(self.idx),
                              mode='wb')

    def get_tar_file(self) -> str:
        return os.path.basename(self.last_file.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_last_file()

    def tell(self):
        return 0

    def readable(self):
        return False

    def writable(self):
        return True

    def seekable(self):
        return False

    def write(self, b, /):
        if self.encrypt_key is not None:
            b = self.chacha.encrypt(b)
        
        self.last_file.write(b)


class DecryptFileObj:
    def __init__(self, filenames, encrypt, encrypt_key):
        self.filenames = filenames
        self.encrypt = encrypt
        #self.aes = AES.new(encrypt_key, mode=AES.MODE_EAX)
        self.chacha = ChaCha20.new(key=encrypt_key)

        self.idx = 0
        self.file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def tell(self):
        return 0
    
    def readable(self):
        return True
    
    def writable(self):
        return False
    
    def seekable(self):
        return False
    
    def read(self, size=-1):
        if self.file is None:
            if self.idx >= len(self.filenames):
                return b''
            self.file = open(self.filenames[self.idx], mode='rb')
            self.idx += 1

        if size == -1:
            data = self.file.read()
            if not data:
                self.file.close()
                self.file = None
            return data

        data = self.file.read(size)
        if not data:
            self.file.close()
            self.file = None
            return self.read(size)

        if self.encrypt:
            data = self.chacha.decrypt(data) #self.aes.decrypt(data)
        return data
