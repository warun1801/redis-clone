from helper import normalize_idx
from command import BaseCommand
from message_formatter import msg
from resp import RESPType

class LRangeCommand(BaseCommand):
    async def execute(self):
        try:
            key = self.arg_list[0]
            start = int(self.arg_list[1])
            end = int(self.arg_list[2])

            if key not in self.cache:
                self.writer.write(msg([], RESPType.ARRAY))
                await self.writer.drain()
                return

            if not isinstance(self.cache[key], list) and not isinstance(self.cache[key], deque):
                self.writer.write(msg("ERR wrong type of value for 'lrange' command", RESPType.ERROR))
                await self.writer.drain()
                return

            lst = self.cache[key]
            n = len(lst)

            start = normalize_idx(start, n)
            end = normalize_idx(end, n)

            if start > end or start >= n:
                self.writer.write(msg([], RESPType.ARRAY))
                await self.writer.drain()
                return

            # Adjust end to be inclusive
            end = min(end, n - 1)
            result = [lst[i] for i in range(start, end + 1)]

            self.writer.write(msg(result, RESPType.ARRAY))
            await self.writer.drain()
            return

        except IndexError:
            self.writer.write(msg("ERR wrong number of arguments for 'lrange' command", RESPType.ERROR))
            await self.writer.drain()
            return

        except ValueError:
            self.writer.write(msg("ERR value is not an integer or out of range", RESPType.ERROR))
            await self.writer.drain()
            return