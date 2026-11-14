"""Variable text still requires a format rule or an actual paired code value."""
from functools import lru_cache


def variable(region):
    return bool(region.get('flexible') or region.get('check'))


def accepts(region, value, paired=False):
    from inspection.special_checks import matches_format
    if not value:
        return False
    if variable(region):
        if region.get('pattern'):
            return matches_format(value,region['pattern'])
        return bool(paired and region.get('flexible') and not region.get('check'))
    return value == region.get('text')


def paired_matches(code, regions, candidates):
    if not code or not regions or len(regions) != len(candidates):
        return False
    @lru_cache(maxsize=None)
    def match(index, offset):
        if index == len(regions):
            return offset == len(code)
        return any(accepts(regions[index],value,paired=True)
                   and code.startswith(value,offset) and match(index+1,offset+len(value))
                   for value in candidates[index])
    return match(0,0)
