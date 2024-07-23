def _wrap_level(f1,level, Dict):
    def wrapped_level(value):
        #print(value)
        res = f1(level,value, Dict)
        if res is None:
            raise ValueError('Generalization step provided has no coverage for this value')

        return res
    
    return wrapped_level

class GenRule():
    def __init__(self, f1,numberOfLevel, Dict):
        id_fn = lambda x: x
        custom_levels = [_wrap_level(f1,lvl, Dict) for lvl in range(numberOfLevel)]
        self._levels = [id_fn] + custom_levels
        self.max_level = len(self._levels) - 1

    def _verify_level(self, level):
        if not isinstance(level, int) or level < 0:
            raise ValueError('Level must be a positive integer')
        if level > self.max_level:
            raise ValueError('Not enough generalization steps available to this rule')
    
    def apply(self, value, level):
        self._verify_level(level)
        return self._levels[level](value)

    def level(self, lvl):
        self._verify_level(lvl)
        return self._levels[lvl]

__all__ = ['GenRule']