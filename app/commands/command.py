from enum import Enum
from abc import ABC, abstractmethod
from asyncio import StreamWriter
from collections import defaultdict


class BaseCommand(ABC):
    def __init__(self, writer: StreamWriter, arg_list: list, cache: dict, time_cache: dict, block_list: defaultdict):
        self.writer = writer
        self.arg_list = arg_list
        self.cache = cache
        self.time_cache = time_cache
        self.block_list = block_list

    @abstractmethod
    async def execute(self, *args, **kwargs):
        pass

class Command(Enum):
    PING = 'ping'
    ECHO = 'echo'
    SET = 'set'
    GET = 'get'
    RPUSH = 'rpush'
    LPUSH = 'lpush'
    LRANGE = 'lrange'
    LLEN = 'llen'
    LPOP = 'lpop'
    RPOP = 'rpop'
    BLPOP = 'blpop'
    TYPE = 'type'
    _ = '_'  # Default case for unrecognized commands

    # Method to get Command enum from string
    @staticmethod
    def from_string(command_str: str):
        for command in Command:
            if command.value == command_str.lower():
                return command
        return Command._
