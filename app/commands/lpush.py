from collections import deque
from command import Command
from message_formatter import msg
from resp import RESPType

class LPushCommand(Command):
    async def execute(self):
        try:
            key = self.arg_list[0]
            values = self.arg_list[1:]

            # initialize the deque if it doesn't exist
            if key not in self.cache:
                self.cache[key] = deque()

            # if the key exists but is not a deque, return an error
            if not isinstance(self.cache[key], deque):
                self.writer.write(msg("ERR wrong type of value for 'lpush' command", RESPType.ERROR))
                await self.writer.drain()
                return

            self.cache[key].extendleft(values)

            self.writer.write(msg(len(self.cache[key]), RESPType.INTEGER))
            await self.writer.drain()
            return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'lpush' command", RESPType.ERROR))
            await self.writer.drain()
            return