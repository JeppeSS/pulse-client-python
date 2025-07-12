import socket
from threading import Thread
import time

class PulseClient:
    def __init__(self, server_ip="127.0.0.1", server_port=3030):
        self.server_addr = (server_ip, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 0)) 
        self.recv_thread = None
        self.ping_thread = None
        self.running = False

        print(f"PulseClient bound to {self.sock.getsockname()}")

    def _build_packet(self, msg_type: int, topic: str, payload: bytes = b'') -> bytes:
        topic_bytes = topic.encode("utf-8")
        topic_len = len(topic_bytes).to_bytes(1, 'big')
        payload_len = len(payload).to_bytes(2, 'big')
        return bytes([msg_type]) + topic_len + payload_len + topic_bytes + payload

    def subscribe(self, topic: str):
        packet = self._build_packet(2, topic)
        self.sock.sendto(packet, self.server_addr)
        print(f"Subscribed to '{topic}'")

    def unsubscribe(self, topic: str):
        packet = self._build_packet(3, topic)
        self.sock.sendto(packet, self.server_addr)
        print(f"Unsubscribed from '{topic}'")

    def publish(self, topic: str, payload: str | bytes):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        packet = self._build_packet(1, topic, payload)
        self.sock.sendto(packet, self.server_addr)
        print(f"Published to '{topic}': {payload.decode('utf-8')}")

    def ping(self):
        packet = self._build_packet(4, "")  # msg_type 4 = Ping
        self.sock.sendto(packet, self.server_addr)
        print("Sent ping.")

    def _ping_loop(self, interval: float = 5.0):
        while self.running:
            time.sleep(interval)
            try:
                self.ping()
            except Exception as e:
                print(f"Ping error: {e}")

    def listen(self, on_message: callable):
        def _recv_loop():
            while self.running:
                try:
                    data, addr = self.sock.recvfrom(1024)
                    on_message(data, addr)
                except Exception as e:
                    print(f"Recv error: {e}")
        self.running = True
        self.recv_thread = Thread(target=_recv_loop, daemon=True)
        self.recv_thread.start()

        self.ping_thread = Thread(target=self._ping_loop, daemon=True)
        self.ping_thread.start()

    def close(self):
        self.running = False
        self.sock.close()
        print("PulseClient closed.")
