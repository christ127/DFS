###############################################################################
#
# Filename: ls.py
# Author: Jose R. Ortiz and Christian J. Esteves Rivera 
#
# Description:
# 	List client for the DFS




import socket
import sys

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.
	so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	

	try:
		#print("Here")
		#print("connected")

		#Creating a packet that will request the list to the meta-data server.
		packet_list_request = Packet()
		#print("Packet request built")
		#Using the function BuildListPacket() from Packet.py,
		#the packet will have the list command inside and it will let know 
		#the meta-data server how to handle the request.
		packet_list_request.BuildListPacket()
		list_request = packet_list_request.getEncodedPacket()
		#Connecting to the socket .
		so.connect((ip,port))
		#sending list request packet to meta-data server.
		so.sendall(list_request)
		#print packet_list_request 
		#print("receiving list from meta-data server")

		#list_response will contain the response from the meta-data server to the
		#list request.
		list_response = so.recv(4096)
		#create a packet to decode and have access to the list that was sent by meta-data.py.
		packet_list = Packet()
		packet_list.DecodePacket(list_response)
		#closing socket connection.
		so.close()
		#printing the file list.
		for file, size in packet_list.getFileArray():
			print """%s %d bytes"""% (file, size)

	except:
		raise

if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int (server[1])

	if not ip:
		usage()

	client(ip, port)
