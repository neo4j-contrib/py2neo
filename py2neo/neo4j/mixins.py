

class Cacheable(object):

    _instances = {}

    @classmethod
    def get_instance(cls, uri):
        if uri not in cls._instances:
            cls._instances[uri] = cls(uri)
        return cls._instances[uri]
