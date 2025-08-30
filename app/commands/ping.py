from command import BaseCommand
from message_formatter import msg
from resp import RESPType

class PingCommand(BaseCommand):
    async def execute(self):
        self.writer.write(msg('PONG', RESPType.SIMPLE_STRING))
        await self.writer.drain()
        return