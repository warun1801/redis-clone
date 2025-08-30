import time

from command import Command
from message_formatter import msg
from resp import RESPType

class GetCommand(Command):
    async def execute(self):
        # get key from cache with the px logic as well 
        try: 
            key = self.arg_list[0]
            curr = int(round(time.time() * 1000))
            if key in self.cache:
                if key in self.time_cache:
                    timestamp, ttl = self.time_cache[key]
                    if curr > timestamp + ttl:
                        # delete expired keys on read
                        del self.cache[key]
                        del self.time_cache[key]

                        self.writer.write(msg(None, RESPType.BULK_STRING))
                        await self.writer.drain()
                        return
                
                # if key exists in cache and not in cache, no ttl so just send it
                self.writer.write(msg(self.cache[key], RESPType.BULK_STRING))
                await self.writer.drain()
                return
            else:
                self.writer.write(msg(None, RESPType.BULK_STRING))
                await self.writer.drain()
                return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'get' command", RESPType.ERROR))
            await self.writer.drain()
            return