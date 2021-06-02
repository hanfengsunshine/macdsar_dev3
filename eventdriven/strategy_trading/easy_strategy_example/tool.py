import zmq
import sys
import json
import argparse

from decimal import Decimal


if __name__ == "__main__":
    monitor = 'ipc:///home/yyy/monitor.ipc'

    request = json.dumps({
        'type': ''
    })
    print(request)

    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 3000)
    socket.connect(monitor)
    socket.send(request.encode())

    try:
        print(socket.recv())
    except:
        print('timeout')
    finally:
        socket.close()
        context.term()
