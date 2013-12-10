package main.edu.columbia.cs6998.sdn.project;

import net.floodlightcontroller.packet.IPv4;

public class Server {

	private String ipAddress;
	private String macAddress;
	private short port;

	public Server(int loadBalancerIp, byte[] loadBalancerMac) {
		ipAddress = IPv4.fromIPv4Address(loadBalancerIp);
		macAddress = new String(loadBalancerMac);
	}

	public Server() {
		// TODO Auto-generated constructor stub
	}
	
	public Server(String ip){
		ipAddress = ip;
	}
	public void setIP(int ip){
		
		ipAddress = IPv4.fromIPv4Address(ip);
	}

	public int getIP() {
		return IPv4.toIPv4Address(ipAddress.getBytes());
	}

	public void setIP(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public short getPort() {
		return port;
	}

	public void setPort(short port) {
		this.port = port;
	}

	public String getMAC() {
		return macAddress;
	}

	public void setMAC(String destMACAddress) {
		macAddress = destMACAddress;
		
	}

}
