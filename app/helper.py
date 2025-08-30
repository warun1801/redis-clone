import asyncio
import time
from collections import deque
END_STR = '\r\n'

async def executor(command: str, arg_list: list, writer: asyncio.StreamWriter, cache: dict, time_cache: dict, block_list) -> None:
    client_name = writer.get_extra_info('peername')
    match command:
        case 'ping':
            writer.write(msg('PONG', 'REPLY'))
            await writer.drain() # waits for the writer to complete completely
            return
        case 'echo':
            for arg in arg_list:
                writer.write(msg(arg, 'ECHO'))
                await writer.drain()
            return
        case 'set':
            cache[arg_list[0]] = arg_list[1]
            if 'px' in [str(i).lower() for i in arg_list]:
                curr = int(round(time.time() * 1000))
                time_cache[arg_list[0]] = (curr, int(arg_list[3]))
                
            writer.write(msg('OK', 'REPLY'))
            await writer.drain()
            return
        case 'get':
            curr = int(round(time.time() * 1000))
            if arg_list[0] in cache:
                if arg_list[0] not in time_cache:
                    writer.write(msg(cache[arg_list[0]], 'ECHO'))
                    await writer.drain()
                else:
                    if sum(time_cache[arg_list[0]]) < curr:
                        # delete expired keys on read
                        del cache[arg_list[0]]
                        del time_cache[arg_list[0]]

                        writer.write(msg('-1', 'SIZE'))
                        await writer.drain()
                        
                    else:
                        writer.write(msg(cache[arg_list[0]], 'ECHO'))
                        await writer.drain()
            else:
                writer.write(msg('-1', 'SIZE'))
                await writer.drain()
            return
        case 'rpush':
            if arg_list[0] not in cache:
                cache[arg_list[0]] = deque([])
            cache[arg_list[0]].extend(arg_list[1:])
            
            writer.write(msg(len(cache[arg_list[0]]), 'RESP'))
            await writer.drain()
            return
        case 'lpush':
            if arg_list[0] not in cache:
                cache[arg_list[0]] = deque([])
            cache[arg_list[0]].extendleft(arg_list[1:])
            
            writer.write(msg(len(cache[arg_list[0]]), 'RESP'))
            await writer.drain()
            return
        case 'lrange':
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
        case 'llen':
            l = 0
            if arg_list[0] in cache:
                l = len(cache[arg_list[0]])
            writer.write(msg(f'{l}', 'RESP'))
            await writer.drain()
            return
        case 'lpop':
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
        case 'rpop':
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
        case 'blpop':
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
