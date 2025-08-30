from enum import Enum

class RESPType(Enum):
    SIMPLE_STRING = '+'
    ERROR = '-'
    INTEGER = ':'
    BULK_STRING = '$'
    ARRAY = '*'
    NULL = '_'
    BOOLEAN = '#'
    DOUBLE = ','
    BIG_NUMBER = '('
    BULK_ERROR = '!'
    VERBATIM_STRING = '='
    MAP = '%'
    ATRRIBUTE = '|'
    SET = '~'
    PUSH = '>'