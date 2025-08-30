import time

from command import Command
from message_formatter import msg
from resp import RESPType

class SetCommand(Command):
    async def execute(self):
        # set key value in cache with the px logic as well 
        try: 
            key = self.arg_list[0]
            value = self.arg_list[1]
            self.cache[key] = value
            if 'px' in [str(i).lower() for i in self.arg_list]:
                curr = int(round(time.time() * 1000))
                self.time_cache[key] = (curr, int(self.arg_list[3]))
            self.writer.write(msg('OK', RESPType.SIMPLE_STRING))
            await self.writer.drain()
            return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'set' command", RESPType.ERROR))
            await self.writer.drain()
            return