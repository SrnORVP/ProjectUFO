import io
from pprint import pprint as pp
from util_Logging import LogWarp as LW
import util_Path as UP


class ContainerIO:
    container = {}

    def __init__(self, verbose=False, log=None):
        self.verbose = verbose
        self._L = log if log is not None else LW

    def __repr__(self):
        pp(self.container)
        return ''

    def allocate(self, path):
        try:
            self.container[path] = io.BytesIO(io.open(path, "rb").read())
        except (FileNotFoundError, TypeError):
            self.container[path] = io.BytesIO()
        if self.verbose:
            print(f'ByteIO allocated at "{hex(id(self.container[path]))}".')

    def retrieve(self, path):
        try:
            self.container[path]
        except KeyError:
            self.allocate(path)
        if self.verbose:
            print(f'ByteIO retrieved at "{hex(id(self.container[path]))}".')
        return self.container[path]

    def save_streams(self):
        for k, v in self.container.items():
            with open(k, 'wb') as file:
                file.write(v.getbuffer())
                repr_string = f'ByteIO "{hex(id(v))}" saved to disk at "{UP.get_rel_path(k, 4)}.'
                LW.wt(repr_string)
                if self.verbose:
                    print(repr_string)

    def byteio_override(self, pathname_container, key):
        verbose = self.verbose
        self.verbose = False
        pn = pathname_container[key]
        self.retrieve(pn)
        pathname_container[key] = self.container[pn]
        self.verbose = verbose
        repr_string = f'ByteIO "{hex(id(self.container[pn]))}" replace "{key}" of "{UP.get_rel_path(pn, 4)}".'
        LW.wt(repr_string)
        if self.verbose:
            print(repr_string)

    def terminate(self):
        self.save_streams()
        for v in self.container.values():
            v.close()
