try:
    import json
except ImportError:
    import simplejson as json

from .rawreducer import RawReducer


class JsonLinesReducer(RawReducer):

    def factory(self):
        return lambda key, values: (json.dumps(v) + '\n' for v in values)
