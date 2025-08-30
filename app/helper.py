import asyncio
from collections import deque

from commands import *

END_STR = '\r\n'

async def executor(command: str, arg_list: list, writer: asyncio.StreamWriter, cache: dict, time_cache: dict, block_list) -> None:
    client_name = writer.get_extra_info('peername')
    command = Command.from_string(command)
    match command:
        case Command.PING:
            await PingCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.ECHO:
            await EchoCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.SET:
            await SetCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.GET:
            await GetCommand(writer, arg_list, cache, time_cache, block_list).execute()
        case Command.RPUSH:
            if arg_list[0] not in cache:
                cache[arg_list[0]] = deque([])
            cache[arg_list[0]].extend(arg_list[1:])
            
            writer.write(msg(len(cache[arg_list[0]]), 'RESP'))
            await writer.drain()
            return
        case Command.LPUSH:
            if arg_list[0] not in cache:
                cache[arg_list[0]] = deque([])
            cache[arg_list[0]].extendleft(arg_list[1:])
            
            writer.write(msg(len(cache[arg_list[0]]), 'RESP'))
            await writer.drain()
            return
        case Command.LRANGE:
            if arg_list[0] not in cache:
                writer.write(msg('0', 'RANGE'))
            else:
                s = [0]
                size = len(cache[arg_list[0]])
                start_idx = normalize_idx(int(arg_list[1]), size)
                end_idx = normalize_idx(int(arg_list[2]), size)

                for i in range(start_idx, min(end_idx + 1, size)):
                    s.append(cache[arg_list[0]][i])
                    s[0] += 1
                s[0] = str(s[0])
                writer.write(msg(','.join(s), 'RANGE'))
            
            await writer.drain()
            return
        case Command.LLEN:
            l = 0
            if arg_list[0] in cache:
                l = len(cache[arg_list[0]])
            writer.write(msg(f'{l}', 'RESP'))
            await writer.drain()
            return
        case Command.LPOP:
            sz = 1
            if len(arg_list) > 1:
                sz = int(arg_list[1])  
            
            if arg_list[0] not in cache or len(cache[arg_list[0]]) == 0:
                writer.write(msg(-1, 'SIZE'))
            else:
                if sz == 1:
                    writer.write(msg(cache[arg_list[0]].popleft(), 'ECHO'))
                else:
                    vals = [0]
                    for _ in range(min(sz, len(cache[arg_list[0]]))):
                        vals.append(cache[arg_list[0]].popleft())
                        vals[0] += 1
                    vals[0] = str(vals[0])
                    writer.write(msg(','.join(vals), 'RANGE'))
            await writer.drain()
            return
        case Command.RPOP:
            sz = 1
            if len(arg_list) > 1:
                sz = int(arg_list[1])  
            
            if arg_list[0] not in cache or len(cache[arg_list[0]]) == 0:
                writer.write(msg(-1, 'SIZE'))
            else:
                if sz == 1:
                    writer.write(msg(cache[arg_list[0]].pop(), 'ECHO'))
                else:
                    vals = [0]
                    for _ in range(min(sz, len(cache[arg_list[0]]))):
                        vals.append(cache[arg_list[0]].pop())
                        vals[0] += 1
                    vals[0] = str(vals[0])
                    writer.write(msg(','.join(vals), 'RANGE'))
            await writer.drain()
            return
        case Command.BLPOP:
            if arg_list[0] in cache and len(cache[arg_list[0]]) > 0:
                vals = ['2', arg_list[0], cache[arg_list[0]].popleft()]
                writer.write(msg(','.join(vals), 'RANGE'))
                await writer.drain()
                return
            # time to wait
            ttw = float(arg_list[1])
            if ttw == 0:
                ttw = float('inf')
            c = 0
            block_list[arg_list[0]].append(client_name)
            while c < ttw:
                await asyncio.sleep(0.1)
                c += 0.1
                if arg_list[0] in cache and len(cache[arg_list[0]]) > 0:
                    if block_list[arg_list[0]][0] == client_name:
                        block_list[arg_list[0]].pop(0)
                        vals = ['2', arg_list[0], cache[arg_list[0]].popleft()]
                        writer.write(msg(','.join(vals), 'RANGE'))
                        await writer.drain()
                        return
            writer.write(msg(-1, 'ARGS'))
            await writer.drain()
            return
        case 'type':
            if arg_list[0] not in cache:
                writer.write(msg('none', 'REPLY'))
            else:
                if type(cache[arg_list[0]]) == str:
                    writer.write(msg('string', 'REPLY'))
                elif type(cache[arg_list[0]]) == deque:
                    writer.write(msg('list', 'REPLY'))
                else:
                    writer.write(msg('string', 'REPLY'))
            await writer.drain()
            return
        case '_':
            writer.write(b'Invalid Argument\r\n')
            await writer.drain()
            return
           
def redis_protocol_parser(data) -> tuple:
    entities = data.decode().split('\r\n')
    if entities[-1] == '':
        entities.pop()
    # print(entities, data)
    args = []
    for i in range(1, len(entities)):
        if str(entities[i]).startswith('$'):
            continue
        args.append(entities[i])

    return (args.pop(0).lower(), args) 

def normalize_idx(idx: int, size: int):
    if idx >= 0:
        return idx
    elif abs(idx) >= size:
        return 0
    else:
        return idx % size

def msg(m, type):
    match type:
        case 'REPLY':
            return f'+{m}{END_STR}'.encode()
        case 'ECHO':
            return f'${len(m)}{END_STR}{m}{END_STR}'.encode()
        case 'ARGS':
            return f'*{m}{END_STR}'.encode()
        case 'SIZE':
            return f'${m}{END_STR}'.encode()
        case 'RESP':
            return f':{m}{END_STR}'.encode()
        case 'RANGE':
            c = str(m).count(',')
            if c == 0:
                return msg(c, 'ARGS')
            else:
                ret_val = msg(c, 'ARGS')
                for i in str(m).split(',')[1:]:
                    ret_val += msg(i, 'ECHO')
                return ret_val
        
        case '_':
            return f''.encode()
