import socket
import threading
from tabulate import tabulate
import glob

import time


class MyThread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        global exit_flag
        if self.threadID == 1:
            ClientUDP(my_ip, my_udp_port)
        elif self.threadID == 2:
            mainThread()
            exit_flag = True
        elif self.threadID == 3:
            discovery()


class ServerTCP:
    pass


class ClientTCP:
    pass


class ServerUDP:
    def __init__(self, UDP_IP, UDP_PORT, bFileName, file_name, isResponse):
        # UDP_IP = "127.0.0.1"
        # UDP_PORT = 5005
        buf = 1024

        # print("UDP target port: %s" % UDP_PORT)
        # print("message: %s" % file_name)

        sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)  # UDP
        if not isResponse:
            sock.sendto(bFileName, (UDP_IP, UDP_PORT))
            # print('sending', file_name)

            with open(file_name, "rb") as f:
                byte = f.read(1024)
                # print("sending this byte:", byte)
                while byte:
                    # Do stuff with byte.
                    sock.sendto(byte, (UDP_IP, UDP_PORT))
                    byte = f.read(1024)
                    time.sleep(0.2)
                sock.sendto(b"end", (UDP_IP, UDP_PORT))
            # print('sending finished')

            f.close()
        elif isResponse:
            tcpPort = findTCPAddress()
            res = my_name + ' ' + my_ip + ' ' + str(tcpPort) + ' ' + 'response'
            sock.sendto(res.encode(), (UDP_IP, int(UDP_PORT)))
            print('response sent')


class ClientUDP:
    def __init__(self, UDP_IP, UDP_PORT):
        timeout = 3
        global responseNode, waitingForResponse, resReceived

        sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)  # UDP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((UDP_IP, UDP_PORT))
        # print('client started')

        while not exit_flag:
            data, addr = sock.recvfrom(1024)
            # print("data received from:", addr)

            if data and not data.endswith(b"get") and not data.endswith(b'response'):
                file_name = "test client" + str(my_udp_port) + ".txt"
                f = open(file_name, 'wb')

                while not exit_flag:
                    data, addr = sock.recvfrom(1024)
                    # print("This data received:", data)
                    if data.endswith(b"end"):
                        # print("client finished")
                        f.close()
                        checkDuplicate(file_name)
                        break
                    if data and not data.endswith(b"txt") and not data.endswith(b"get"):
                        if not data.endswith(b'response'):
                            f.write(data)
            if data and data.endswith(b"get"):
                print("first get received")
                data, addr = sock.recvfrom(1024)
                if data and data.endswith(b"sget"):
                    print("second get received")
                    receivedData = str(data)
                    receivedData = receivedData.split(' ')
                    if receivedData[3] in files:
                        if receivedData[0] not in responseNode:
                            time.sleep(0.2)  # piggybacking

                            responseNode.add(receivedData[0])
                        print("i have it")
                        sendResponse(receivedData[0:-1])
            if data and data.endswith(b"response") and waitingForResponse:
                waitingForResponse = False
                resReceived = True
                splitData = data.decode().split(' ')
                print("gettin from" + str(splitData[0]))
                files.add()


def findIP(file_name):
    cluster = []
    f = open(file_name, "r")
    for x in f:
        inp = x.split(' ')
        cluster.append(inp[1:])
    return cluster


def sendDiscovery(byte_file_name, file_name):
    try:
        cluster = findIP(file_name)
        for x in cluster:
            ServerUDP(x[0], int(x[1]), byte_file_name, file_name, False)
    except:
        print("no ip found")


def findNodes():
    global clusterNodes
    file = open(my_file, 'r')
    for x in file:
        fSplit = x.split(' ')
        clusterNodes.add(fSplit[0])

    file.close()


def checkDuplicate(file_name):
    testFile = open(file_name, 'r')
    myFile = open(my_file, 'a')
    for x in testFile:
        splitLine = x.split(' ')
        if splitLine[0] not in clusterNodes:
            myFile.write(x)
            clusterNodes.add(splitLine[0])
    testFile.close()
    myFile.close()


def discovery():
    print('discovery started')
    while not exit_flag:
        # print(i)

        sendDiscovery(my_file.encode(), my_file)

        time.sleep(5)


def showClusterList():
    clusterFile = open(my_file, 'r')
    nList = []
    for x in clusterFile:
        xSplit = x.split(' ')
        if xSplit[0] != my_name:
            nList.append(xSplit)
    print(tabulate(nList, ['Name', 'IP', 'Port'], tablefmt='psql'))
    clusterFile.close()


def mainThread():
    global exit_flag
    print("enter a number: 1.List 2.Get 3.End")
    while not exit_flag:
        inp = int(input())
        if inp == 1:
            showClusterList()
        if inp == 2:
            print("Enter file name: ")
            file_name = input()
            sendGet(file_name)
        if inp == 3:
            print("Good Bye")
            exit_flag = True


def sendResponse(data):
    ServerUDP(data[1], data[2], None, my_name, True)


def findTCPAddress():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    print("TCP PORT IS:", port)
    return port


def sendGet(file_name):
    global resReceived, waitingForResponse
    waitingForResponse = True
    resReceived = False
    file = open(my_file, 'r')
    for x in file:
        x = x.split(' ')
        if x[0] != my_name:
            ip, port = x[1], int(x[2])
            sock = socket.socket(socket.AF_INET,  # Internet
                                 socket.SOCK_DGRAM)  # UDP
            sock.sendto(b"get", (ip, port))
            time.sleep(0.02)
            req = my_name + ' ' + my_ip + ' ' + str(my_udp_port) + ' ' + file_name + ' ' + 'sget'
            sock.sendto(req.encode(), (ip, port))
            print("get request sent")
            time.sleep(0.2)
            time.sleep(resTimeOut)  # timeout for response
            if not resReceived:
                print("File not found")

    sock.close()


exit_flag = False
clusterNodes = set()
files = set()
resReceived = False
resTimeOut = 1
responseNode = set()
waitingForResponse = False

print("Enter node name:")
my_name = input()
print("Enter UDP IP:")
my_ip = input()
print("Enter UDP PORT:")
my_udp_port = int(input())
print("enter your cluster file name (wait a moment)")
my_file = input()

##############
for f in glob.glob('*'):
    files.add(f)
    print(f)

findNodes()
time.sleep(5)
clientThread = MyThread(1, "client")
myMainThread = MyThread(2, "main")
discoveryThread = MyThread(3, "discovery")
clientThread.start()
discoveryThread.start()
myMainThread.start()
clientThread.join()
discoveryThread.join()
myMainThread.join()
