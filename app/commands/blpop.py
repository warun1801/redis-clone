import asyncio

from command import Command
from message_formatter import msg
from resp import RESPType

class BLPopCommand(Command):
    async def execute(self):
        try:
            key = self.arg_list[0]

            if key in self.cache and len(self.cache[key]) > 0:
                value = self.cache[key].popleft()
                self.writer.write(msg([key, value], RESPType.ARRAY))
                await self.writer.drain()
                return
            
            ttw = float(self.arg_list[1])
            if ttw == 0:
                ttw = float('inf')
            c = 0
            client_name = f"{self.writer.get_extra_info('peername')}"
            self.block_list[key].append(client_name)
            while c < ttw:
                await asyncio.sleep(0.1)
                c += 0.1
                # If the key has values now
                if key in self.cache[key] and len(self.cache[key]) > 0:
                    # you are the first in line, pop the value
                    if self.block_list[key][0] == client_name:
                        self.block_list[key].pop(0)
                        value = self.cache[key].popleft()
                        self.writer.write(msg([key, value], RESPType.ARRAY))
                        await self.writer.drain()
                        return

            # timeout reached, return nil array
            self.writer.write(msg(None, RESPType.ARRAY))
            await self.writer.drain()
            return
        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'blpop' command", RESPType.ERROR))
            await self.writer.drain()
            return