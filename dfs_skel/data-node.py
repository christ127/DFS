###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and Christian J. Esteves Rivera
# studentnum: 801-14-2188
# Description:
# data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	sock.connect((meta_ip, meta_port))

	
	# Fill code	

	try:
		response = "NAK"
		# creating packet to send to the meta-data server. 
		packet_reg = Packet()
		while response == "NAK":
			#The packet will contain the address and the port from the datanode to be registered
			packet_reg.BuildRegPacket(data_ip, data_port)
			#send packet to meta-data server.
			sock.sendall(packet_reg.getEncodedPacket())
			response = sock.recv(1024)

			if response == "ACK":
				print "DataNode was registered successfully"
			if response == "DUP":
				print "Duplicate Registration"

		 	if response == "NAK":
				print "Registration ERROR"

	finally:
		sock.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""
		fname, fsize = p.getFileInfo()
		
	
		self.request.send("OK")

		#Recieving data from the file that will be stored in the file system
		#from copy.py.


		data = self.request.recv(2**16)
		print (data)
		
		print("entering the block stage")
		# Generates an unique block id.
		blockid = str(uuid.uuid1())

		#print(blockid)
		# Open the file for the new data block.  
		fileopen = open(DATA_PATH + "/" + blockid, 'a+')
		#writing the data in the datablock.
		fileopen.write(data)
		fileopen.close()
		# Send the block id back to copy.py.
		self.request.sendall(blockid)
		self.request.close()



	def handle_get(self, p):
		
		# Get the block id from the packet sent from copy.py.
		blockid = p.getBlockID()
		#print("we in  handle_get")
		#opening file using the given path and the blockid.
		with open(DATA_PATH + "/" + blockid) as file:
			#send file back to copy.py.
			self.request.sendall(file.read())
		self.request.close()


		# Read the file with the block id data
		# Send it back to the copy client.
		
		# Fill code

	def handle(self):
		msg = self.request.recv(1024)
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			print("handling get")
			self.handle_get(p)

		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()
	except:
		usage()


	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
 	server.serve_forever()
