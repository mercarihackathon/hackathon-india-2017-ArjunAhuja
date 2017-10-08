import socket                                         
import time
import thread
from threading import Thread
import mysql.connector
from mysql.connector import errorcode
import psycopg2
import sys
import math

# people/policeman under a sensor
expectedRatio = 5
people = 0

input_port = [] #in increasing distance relevance

for i in range(2):
	input_port.append(input())

#connect to a database

try:
	cnn = mysql.connector.connect(
		user = 'root',
		password = 'arjun',
		host = 'localhost',
		database = 'mercari'
		)
except mysql.connector.Error as e:
	if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
		print "something is wrong with the username/password"
	elif e.errno == errorcode.ER_BAD_DB_ERROR:
		print "database does not exist"
	else:
		print e

#handling Police database operations

def addPoliceToDatabase(var):
	addNewQuery = ("INSERT INTO " + sys.argv[1] + "(port) values(%s)"%(int(var)))
	cursor = cnn.cursor()
	cursor.execute(addNewQuery)
	cnn.commit()
	cursor.close()

def DeletePoliceFromDatabase():
	addNewQuery = ("DELETE FROM " + sys.argv[1] + " ORDER BY id ASC LIMIT 1")
	cursor = cnn.cursor()
	cursor.execute(addNewQuery)
	cnn.commit()
	cursor.close()

def RetrievePoliceFromDatabase():
	addNewQuery = ("Select * FROM " + sys.argv[1] + " ORDER BY id ASC LIMIT 1")
	cursor = cnn.cursor()
	cursor.execute(addNewQuery)
	policeID = 0
	for row in cursor:
		policeID = row[1]
	cnn.commit()
	cursor.close()
	return policeID

def countPoliceDatabase():
	addNewQuery = ("Select COUNT(*) FROM " + sys.argv[1])
	cursor = cnn.cursor()
	cursor.execute(addNewQuery)
	number_of_police = 0
	for row in cursor:
		number_of_police = row[0]
	cnn.commit()
	cursor.close()
	return number_of_police

#checks if current number of people are more than number of policemen
def checkingFunction():
	global people
	while(True):
		inp,shouldAdd = input(),input()
		if shouldAdd == 1:
			people += inp
			print "people updated to ",people
			countPolice = countPoliceDatabase()
			left = math.ceil(people/expectedRatio) - countPolice
			#more police are needed
			if left>0:
				#send messages to recieving end of other nodes
				currentNode = 0
				while left>0:
					# create a socket object
					sock = socket.socket(
						        socket.AF_INET, socket.SOCK_STREAM)                          

					sock.settimeout(3) # 3 second timeout on commands

					#port of send and recieve for the checking function
					port = int(sys.argv[3])
					sock.connect(('',input_port[currentNode%(len(input_port))]))
					sock.sendall(str(0))
					data = sock.recv(4096)
					if int(data) > 0:
						addPoliceToDatabase((int(data)))
						left-=1
					else:
						currentNode+=1
					sock.close()
				print "number of policeman updated to ",countPoliceDatabase()
		else:
			people -= inp
			print "people updated to ",people

# create a socket object
sock = socket.socket(
	        socket.AF_INET, socket.SOCK_STREAM) 

# get local machine name
host = socket.gethostname()                           

port = int(sys.argv[2]) 

new_thread = Thread(target = checkingFunction)
new_thread.setDaemon(True)
new_thread.start()      

sock.bind(('', port))  

# put the socket into listening mode
sock.listen(10)     
print "socket is listening"  

while True:
	# Establish connection with client.
	c, addr = sock.accept()     
	print 'Got connection from', addr

	#send message to the one who asked here by adding the c,addr to the queue
	rec = c.recv(1024)

	if math.ceil(people/expectedRatio) <= countPoliceDatabase() :
		c.send(str(RetrievePoliceFromDatabase()))
		DeletePoliceFromDatabase()
		print "number of policeman updated to ",countPoliceDatabase()
	else:
		c.send("-1")
	# Close the connection with the client
	c.close()