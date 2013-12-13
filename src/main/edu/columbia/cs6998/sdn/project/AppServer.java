package main.edu.columbia.cs6998.sdn.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

public class AppServer {
	private String ip;
	private short port;


	public short getPortClass(){
		return (short) ((port / 10) * 10);
	}
	public AppServer(String ip, short port) {
		// TODO Auto-generated constructor stub
		this.ip = ip;
		this.port = port;
	}



	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public short getPort() {
		return port;
	}
	public void setPort(short port) {
		this.port = port;
	}
}