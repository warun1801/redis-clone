from collections import deque

from command import Command
from message_formatter import msg
from resp import RESPType

class TypeCommand(Command):
    async def execute(self):
        try:
            key = self.arg_list[0]

            if key not in self.cache:
                self.writer.write(msg("none", RESPType.SIMPLE_STRING))
                await self.writer.drain()
                return

            value = self.cache[key]
            if isinstance(value, str):
                type_str = "string"
            elif isinstance(value, list):
                type_str = "list"
            elif isinstance(value, deque):
                type_str = "list"  # Redis treats deques as lists
            elif isinstance(value, set):
                type_str = "set"
            elif isinstance(value, dict):
                type_str = "hash"
            else:
                type_str = "unknown"

            self.writer.write(msg(type_str, RESPType.SIMPLE_STRING))
            await self.writer.drain()
            return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'type' command", RESPType.ERROR))
            await self.writer.drain()
            return