
import os
import shutil

def deleteFolderContents(target_dir, delete_tree=False):
    with os.scandir(target_dir) as entries:
        for entry in entries:
            if entry.is_dir() and not entry.is_symlink():
                if delete_tree:
                    shutil.rmtree(entry.path)
            else:
                os.remove(entry.path)

class SimpleFilesystemCacher:

    base = None

    def __init__(self, base="./files"):
        self.setBase(base)

    def getBase(self):
        return self.base

    def setBase(self, base):
        self.base = base
        self.connectBase()

    def connectBase(self):
        if not os.path.exists(self.base):
            os.makedirs(self.base)

    def getKeyFilename(self, key):
        return os.path.join(self.base, key)

    def set(self, key, binary_value):
        with open(self.getKeyFilename(key), "wb") as file:
            file.write(binary_value)

    def get(self, key):
        if os.path.exists(self.getKeyFilename(key)):
            with open(self.getKeyFilename(key), "rb") as file:
                return file.read()
        return None

    def getAll(self):
        with os.scandir(self.base) as entries:
            for entry in entries:
                yield entry.name, self.get(entry.name)

    def importFrom(self, from_cacher=None):
        for item in from_cacher.getAll():
            self.set(item[0], item[1])

    def clear(self, delete_base=False):
        if os.path.exists(self.base):
            deleteFolderContents(self.base)
        if delete_base:
            os.rmdir(self.base)

