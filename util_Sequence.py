import io
from pprint import pprint as pp


class SequenceByteIO:
    container = {}

    def __init__(self, verbose=False):
        self.verbose = verbose

    def __repr__(self):
        pp(self.container)

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
                if self.verbose:
                    print(f'ByteIO "{hex(id(v))}" saved to disk.')

    def byteio_override(self, pathname_container, container_key):
        verbose = self.verbose
        self.verbose = False
        pn = pathname_container[container_key]
        self.retrieve(pn)
        pathname_container[container_key] = self.container[pn]
        self.verbose = verbose
        if self.verbose:
            print(f'"{container_key}" replaced by ByteIO "{hex(id(self.container[pn]))}".')

    def terminate(self):
        self.save_streams()
        for v in self.container.values():
            v.close()
