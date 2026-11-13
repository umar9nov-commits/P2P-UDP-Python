
import socket

# Receiver IP address and UDP port
HOST = "127.0.0.1"
PORT = 5001

# Create a UDP socket
receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind the socket to the receiver address
receiver.bind((HOST, PORT))

print("UDP receiver is waiting on port 5001...")

# Wait for a UDP message
data, address = receiver.recvfrom(1024)

# Convert received bytes into readable text
message = data.decode("utf-8")

print("Message received:", message)
print("Sender address:", address)

# Close the socket
receiver.close()
