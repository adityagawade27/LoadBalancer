package main.edu.columbia.cs6998.sdn.project;

import net.floodlightcontroller.packet.IPv4;

public class Server {

	private String ipAddress;
	private String macAddress;
	private short port;

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
