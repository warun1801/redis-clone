from command import BaseCommand
from message_formatter import msg
from resp import RESPType

class EchoCommand(BaseCommand):
    async def execute(self):
        for arg in self.arg_list:
            self.writer.write(msg(arg, RESPType.BULK_STRING))
            await self.writer.drain()
        return
