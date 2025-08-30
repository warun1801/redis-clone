from command import Command
from message_formatter import msg
from resp import RESPType

class LLenCommand(Command):
    async def execute(self):
        try:
            key = self.arg_list[0]

            if key not in self.cache:
                self.writer.write(msg(0, RESPType.INTEGER))
                await self.writer.drain()
                return

            if not isinstance(self.cache[key], list):
                self.writer.write(msg("ERR wrong type of value for 'llen' command", RESPType.ERROR))
                await self.writer.drain()
                return

            self.writer.write(msg(len(self.cache[key]), RESPType.INTEGER))
            await self.writer.drain()
            return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'llen' command", RESPType.ERROR))
            await self.writer.drain()
            return