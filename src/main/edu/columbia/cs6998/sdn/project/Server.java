package main.edu.columbia.cs6998.sdn.project;


public class Server {

	private String ipAddress;
	private String macAddress;
	private short port;
	private short tcpPort;

	public short getTcpPort() {
		return tcpPort;
	}

	public void setTcpPort(short tcpPort) {
		this.tcpPort = tcpPort;
	}

	public Server(String ipAddress, String macAddress) {
		super();
		this.ipAddress = ipAddress;
		this.macAddress = macAddress;
	}

	public Server() {
		// TODO Auto-generated constructor stub
	}

	public String getIP() {
		return ipAddress;
	}

	public void setIP(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getMAC() {
		return macAddress;
	}

	public void setMAC(String macAddress) {
		this.macAddress = macAddress;
	}

	public short getPort() {
		return port;
	}

	public void setPort(short port) {
		this.port = port;
	}
	
	

}
