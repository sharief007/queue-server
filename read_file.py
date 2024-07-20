import zlib

with open('./data/queue-test/queue.bin', mode='rb') as f:
    while True:
        x = f.read(4)
        x = int.from_bytes(x,'big')
        print(x)
        con = f.read(x)
        con = zlib.decompress(con)
        print(con.decode('utf-8'))
