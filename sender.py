
import socket

# Receiver IP address and port
HOST = "127.0.0.1"
PORT = 5001

# Create UDP socket
sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Prepare message
message = "Hello from Python UDP Sender!"

# Send message
sender.sendto(message.encode("utf-8"), (HOST, PORT))

print("Message sent:", message)

# Close socket
sender.close()
