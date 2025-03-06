import os
import os.path
import argparse
from glob import iglob
from contextlib import suppress

def remove_file_ignore_errors(filename):
    with suppress(OSError):
        os.remove(filename)

def listfiles_recursive_iter(folder):
    for file_or_dir in iglob(os.path.join(folder, '**'),
                             recursive=True,
                             include_hidden=True):
        if os.path.isfile(file_or_dir) and not os.path.islink(file_or_dir):
            yield os.path.abspath(file_or_dir)

class ValidateFileExists(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        if not os.path.isfile(values):
            raise argparse.ArgumentError(self, f"The file '{values}' doesn't exist!")
        setattr(namespace, self.dest, os.path.abspath(values))

class ValidateFolderExists(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        if not isinstance(values, list):
            values = [values]

        for folder in values:
            if not os.path.isdir(folder):
                raise argparse.ArgumentError(self, f"The folder '{folder}' doesn't exist!")
        setattr(namespace, self.dest, [os.path.abspath(x) for x in values])