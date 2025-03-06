import os
from Cryptodome.Cipher import ChaCha20


class EncryptSplitFileObj:
    def __init__(self,
                 filename: str,
                 encrypt_key: bytes,
                 split_size: int,
                 callback: callable):
        self.filename = filename
        self.encrypt_key = encrypt_key
        self.split_size = split_size
        self.callback = callback

        if encrypt_key is not None:
            self.chacha = ChaCha20.new(key=encrypt_key)
        self.internal_buffer = bytearray()
        self.idx = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.internal_buffer:
            self._write_chunk(self.internal_buffer, is_last=True)

    def tell(self):
        return 0
    
    def readable(self):
        return False
    
    def writable(self):
        return True
    
    def seekable(self):
        return False
    
    def _chunks(self):
        while len(self.internal_buffer) >= self.split_size:
            chunk = self.internal_buffer[:self.split_size]
            self.internal_buffer = self.internal_buffer[self.split_size:]
            yield chunk

    def _write_chunk(self, chunk, is_last=False):
        chuck_filename = f"{self.filename}.{self.idx}"
        os.makedirs(os.path.dirname(chuck_filename), exist_ok=True)
        
        with open(chuck_filename, mode='wb') as f:
            f.write(chunk)
        
        self.callback(chuck_filename, is_last)
        self.idx += 1
    
    def write(self, b, /):
        if self.encrypt_key is not None:
            b = self.chacha.encrypt(b)
        self.internal_buffer += b

        for chunk in self._chunks():
            self._write_chunk(chunk)


class JoinUnencryptFileObj:
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