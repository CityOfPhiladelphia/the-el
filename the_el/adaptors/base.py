import re

class BaseAdaptor(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy_connection()

    def destroy_connection(self):
        raise NotImplementedError()
