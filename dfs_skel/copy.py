###############################################################################
#
# Filename: copy.py
# Author: Jose R. Ortiz and Christian J. Esteves Rivera
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server
	#print("Till here its fine")
	#Getting Filename, Filesize and sending metadata.py for next steps
	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
	#opening socket to connect to meta-data server.
	so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# print("connected")
	#Getting file size to be able to use BuildPutPacket()
	fsize = os.path.getsize(path)
	#Creating packet to send request to meta-data server.
	packet_put_request = Packet()
	packet_put_request.BuildPutPacket(fname,fsize)
	request = packet_put_request.getEncodedPacket()
	#Connection to meta-data server.
	so.connect(address)
	#Sending request to meta-data server.
	so.sendall(request)

	# If no error or file exists
	# Get the list of data nodes.
	#Receiving DataNode list from meta-data server.
	response = so.recv(4096)
	print("message recieved")
	#creating packet to access the DataNode list.
	packet_put_response = Packet()
	#print(response)
	packet_put_response.DecodePacket(response)
	so.close()	
	#print("Now we have dataNode list")
	#Getting dataNodes from the packet.
	DataNodes = packet_put_response.getDataNodes()
	#Getting the number of DataNodes to later calculate the block size.
	num_nodes = len(DataNodes)
	
	#Dividing the file in blocks 
	#opening second socket to connect to the dataNodes. 
	# Fill code
	#Calculating the block size.
	bsize = (fsize / num_nodes) + 1

	# Send the blocks to the data servers
	# Fill code
	#Reading File
	with open(path , 'r+b') as file:
		msg = str(file.read())
	buff_size = 64000
		# print(msg)i*(bsize): (i+1)*(bsize)
	#This will keep track of the blockid's that the file will use.
	blocid = []
	#This for will connect with each Datanode registered in the database and it will
	#send the file divided in blocks.
	for i in range(num_nodes):
		print("second socket opening")
		#opening connection with data-node.py
		so2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		#getting the DataNode that will be used
		Dnode = DataNodes.pop(0)
		# Read file
		# Fill code
		#Creating packet to send to the DataNode the file information.
		dblocpac = Packet()
		dblocpac.BuildPutPacket(fname,fsize)
		dblocksend = dblocpac.getEncodedPacket()
		#connecting to the DataNode 
		so2.connect(tuple(Dnode))   #address,port from the dataNodes.
		so2.sendall(dblocksend)
		#print("reading file")
		

		#print("Message sent")
		block_response = so2.recv(4096)
		if block_response == "OK":
			# print(msg[i*(fsize/num_nodes): (i+1)*(fsize/num_nodes)])
			#Dividing File into chunks and sending it to the DataNode.

			if i == num_nodes:
				bsize = fsize % num_nodes
				msg = msg[i*(bsize): (i+1)*(bsize)]
				so2.sendall(msg)


			else:

				so2.sendall(msg[i*(bsize): (i+1)*(bsize)])

			#Receiving the block id.
			this_block = so2.recv(4096)
			#print("block received")
			#print(this_block)
			#Storing the block id with the given DataNode in the blocid list.
			blocid.append((Dnode[0],Dnode[1],this_block))
		else:
			print("ERROR In Data Node")
	so2.close()


	# Notify the metadata server where the blocks are saved.
	#Receiving block id 
	# Fill code	
	#print(blocid)
	#print("building packet")
	#creating packet to send to meta-data server.
	blocpac = Packet()
	blocpac.BuildDataBlockPacket(fname,blocid)
	packsend = blocpac.getEncodedPacket()
	#opening connection with meta-data server.
	so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	so.connect(address)
	#print("sending blockids")
	so.sendall(packsend)
	
	so.close()

	# Fill code
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""
	# Contact the metadata server to ask for information of fname
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	#connecting to meta-data server.
   	sock.connect(address) 
   	#building packet with the fname and the command get.
	p = Packet()
	p.BuildGetPacket(fname)
	sock.sendall(p.getEncodedPacket())

	#print("Received")
	#Receiving meta-data server response
	respond = sock.recv(4096)
	#print("you've got mail:", respond)
	#If the file is in the inode table in the database, then it look for the
	#Datanode information.
	if respond != "NFOUND":
		packetres = Packet()
		packetres.DecodePacket(respond)
		print("Datablocks information decoded")
		DataNodes = packetres.getDataNodes()
		with open(path,"a+b") as file:
			for n in DataNodes:
				soky = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				print(n[:2])
				n,m = tuple(n[:2]), n[2]
				#print("server:", n, "chunk_ids:",m)
				soky.connect(n)
				packety = Packet()
				packety.BuildGetDataBlockPacket(m)
				
				soky.sendall(packety.getEncodedPacket())
				print("Packet request sent")

				msg = soky.recv(2**16)
				print("Packet received")
				# print(msg)
				soky.close()
				file.write(msg)
				print("writing to file")
		print("Writing DONE!!!!")
	else:
		print(fname, ": File not found!")

	


	# Fill code

	# If there is no error response Retreive the data blocks

	# Fill code

    	# Save the file
	
	# Fill code

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


