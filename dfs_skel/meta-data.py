###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and Christian J. Esteves Rivera
#StudentNum: 801-14-2188
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import *
from Packet import *
import sys
import SocketServer

def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)


class MetadataTCPHandler(SocketServer.BaseRequestHandler):


#Function that will register the Datanodes in the database.
#Uses the database function AddDataNode() from ms_db.py

	def handle_reg(self, db, p):
		"""Register a new client to the DFS  ACK if successfully REGISTERED
			NAK if problem, DUP if the IP and port already registered
		"""
		try:
			#Getting Address and Port from packet received.
			Address = p.getAddr()
			Port = p.getPort()
			#If the DataNode can be stored in the Database then send a ACK to the data-node.py.
			#If it can't be stored, then it is because it is already in the dnode table.
			if db.AddDataNode(Address,Port): # Fill condition:
				self.request.sendall("ACK") 
				print("DataNode Registered")
			else:
				#Send to data-node.py it is a duplicate
				self.request.sendall("DUP")
		except:
			self.request.sendall("NAK")

	def handle_list(self, db):
		"""Get the file list from the database and send list to client"""
		try:
			# Fill code here
			# print("Looking for files in db") 
			#Getting files from the inode table in the database using the GetFiles()
			#function from the mds_db.py.
			lis = db.GetFiles()
			#creating packet to send the file list back to the client.
			paq = Packet()
			# print("Buiding response")
			paq.BuildListResponse(lis)
			coso = paq.getEncodedPacket()	
			self.request.sendall(coso)
			# print("response sent")

		except:
			self.request.sendall("NAK")	

	def handle_put(self, db, p):
		"""Insert new file into the database and send data nodes to save
		   the file.
		"""
		# Fill code
	    #getting fname, fsize from packet using the getFileInfo() function from Packet.py.	
		#Storing the file in the database using the function InsertFile from mds_db.py.
		if db.InsertFile(p.getFileInfo()[0],p.getFileInfo()[1]): 
			# Fill code

			print("file inserted")
			#creating response for the copy client.
			print("packet creation begin")
			packet_send = Packet()
			packet_send.BuildPutResponse(db.GetDataNodes()) #db.GetDataNodes() gives me the data node list for the copy client.
			packet_response = packet_send.getEncodedPacket()  #encoding packet to send to the copy client.
			self.request.sendall(packet_response)
			#print("packet sent")

			pass
		else:
			self.request.sendall("DUP")
	
	def handle_get(self, db, p):
		"""Check if file is in database and return list of
			server nodes that contain the file.
		"""
		
		# Fill code to get the file name from packet and then 
		# get the fsize and array of metadata server
		# Fill code
		#print("Getting file location info: ", p.getFileName())

		#Using GetFileInode() function to recieve the filesize and the 
		#metalist that contains the amount of that contain the file data.
		fsize, metalist = db.GetFileInode(p.getFileName())
		#if the file is in the inode table then send the metalist and the filesize back to the client
		if fsize:
			#print("creating packet")
			#creating the response packet
			packet_send_copy = Packet()
			packet_send_copy.BuildGetResponse(metalist,fsize)
			#print("packet Response built:",metalist,fsize)
			copy_response = packet_send_copy.getEncodedPacket()
			self.request.sendall(copy_response)
			#print("packet sent")
		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Fill code to get file name and blocks from
		# packet
		# Fill code to add blocks to file inode

		#Using the AddBlockToInode function from mds_db.py to add the file blocks
		#to the block table in the database.
		#Getting File name and its datablocks from the Packet that arrives from the copy.py.
		#Using getFileName() and getDataBlocks() from Packet.py.
		if db.AddBlockToInode(p.getFileName(),p.getDataBlocks()):
			print("Blocks added")
		else:
			print("nothing added")
#Each packet that arrives to the meta-data server goes through here first.
#Depending on the command it will go to the appropriate function.
	def handle(self):
		print "Packet received, in handle"
		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)
	

		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files
			# Fill code
			print("Handling list request")
			self.handle_list(db)
		elif cmd == "put":
			# Client asking for servers to put data
			# Fill code
			print("Handling put request")
			self.handle_put(db, p)

		elif cmd == "get":
			# Client asking for servers to get data
			# Fill code
			print("Handling get request")
			self.handle_get(db,p)
			
		elif cmd == "dblks":
			# Client sending data blocks for file
			# Fill code
			print("handling dblks request")
			self.handle_blocks(db,p)
			

		db.Close()

if __name__ == "__main__":
	HOST, PORT = "", 8000

	if len(sys.argv) > 1:
		try:
			PORT = int(sys.argv[1])
		except:
			usage()

	server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

	# Activate the server; this will keep running until you
	# interrupt the program with Ctrl-C
	server.serve_forever()
