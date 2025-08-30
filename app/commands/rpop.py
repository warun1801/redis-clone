from command import Command
from message_formatter import msg
from resp import RESPType
from collections import deque

class RPopCommand(Command):
    async def execute(self):
        try:
            key = self.arg_list[0]

            # if the key doesn't exist, return nil
            if key not in self.cache:
                self.writer.write(msg(None, RESPType.BULK_STRING))
                await self.writer.drain()
                return
            
            # if the key exists but is not a deque, return an error
            if not isinstance(self.cache[key], deque):
                self.writer.write(msg("ERR wrong type of value for 'rpop' command", RESPType.ERROR))
                await self.writer.drain()
                return
            
            sz = 1
            if len(self.arg_list) > 1:
                sz = int(self.arg_list[1])

            popped_values = []
            for _ in range(min(sz, len(self.cache[key]))):
                popped_values.append(self.cache[key].pop())

            if sz == 1:
                result = popped_values[0] if popped_values else None
                self.writer.write(msg(result, RESPType.BULK_STRING))
            else:
                self.writer.write(msg(popped_values, RESPType.ARRAY))

            await self.writer.drain()
            return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'rpop' command", RESPType.ERROR))
            await self.writer.drain()
            return