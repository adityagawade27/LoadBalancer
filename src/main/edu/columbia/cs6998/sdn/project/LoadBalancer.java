package main.edu.columbia.cs6998.sdn.project;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;

import org.openflow.protocol.OFError;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFFlowRemoved;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionDataLayerDestination;
import org.openflow.protocol.action.OFActionNetworkLayerDestination;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.util.HexString;
import org.openflow.util.U16;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module to perform round-robin load balancing.
 * 
 */
public class LoadBalancer implements IOFMessageListener, IFloodlightModule {

	// Interface to Floodlight core for interacting with connected switches
	protected IFloodlightProviderService floodlightProvider;
	
	// Interface to the logging system
	protected static Logger logger;
	
	// IP and MAC address for our logical load balancer
	private final static int LOAD_BALANCER_IP = IPv4.toIPv4Address("10.0.0.254");
	private final static byte[] LOAD_BALANCER_MAC = Ethernet.toMACAddress("00:00:00:00:00:FE");
	
	// Rule timeouts
	private final static short IDLE_TIMEOUT = 60; // in seconds
	private final static short HARD_TIMEOUT = 0; // infinite
	
	private Map<Short, ArrayList<Server>> portNumberServersMap= new HashMap<Short, ArrayList<Server>>();
	private Map<Short, TreeMap<String, Integer>> portServerPacketCountMap;
	private ArrayList<Server> appServers;

	
	private Topology topology;
	private int lastServer = 0;
	
	
	/**
	 * Provides an identifier for our OFMessage listener.
	 * Important to override!
	 * */
	@Override
	public String getName() {
		return LoadBalancer.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// Auto-generated method stub
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// Auto-generated method stub
		return null;
	}

	/**
	 * Tells the module loading system which modules we depend on.
	 * Important to override! 
	 */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService >> floodlightService = 
			new ArrayList<Class<? extends IFloodlightService>>();
		floodlightService.add(IFloodlightProviderService.class);
		return floodlightService;
	}

	/**
	 * Loads dependencies and initializes data structures.
	 * Important to override! 
	 */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		logger = LoggerFactory.getLogger(LoadBalancer.class);
		portServerPacketCountMap = new HashMap<Short, TreeMap<String, Integer>>();
		
		topology = Topology.getInstance();
		initializeAppServers();
	}

	public void initializeAppServers(){
		
		appServers = new ArrayList<Server>();
		appServers.add(new Server("10.0.0.1",topology.getMacAddressFromIP("10.0.0.1")));
		appServers.add(new Server("10.0.0.5",topology.getMacAddressFromIP("10.0.0.5")));
		appServers.add(new Server("10.0.0.6",topology.getMacAddressFromIP("10.0.0.6")));
		appServers.add(new Server("10.0.0.7",topology.getMacAddressFromIP("10.0.0.7")));
		
		portNumberServersMap.put((short)8080,appServers);
	}
	
	/*
	 * Tells the Floodlight core we are interested in PACKET_IN messages.
	 * Important to override! 
	 * */
	@Override
	public void startUp(FloodlightModuleContext context) {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		
	}
	
	/**
	 * Receives an OpenFlow message from the Floodlight core and initiates the appropriate control logic.
	 * Important to override!
	 */
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		switch (msg.getType()) {
		case PACKET_IN:
			try {
				return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		case FLOW_REMOVED:
			try {
				return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
			} catch (Exception e) {
				e.printStackTrace();
			}

		case ERROR:
			logger.info("received an error {} from switch {}", (OFError) msg, sw);
			return Command.CONTINUE;
		
		default:
			break;
		}
		logger.error("received an unexpected message {} from switch {}", msg, sw);
		return Command.STOP;
	}
    
	
//	String to int:

		int pack(byte[] bytes) {
		  int val = 0;
		  for (int i = 0; i < bytes.length; i++) {
		    val <<= 8;
		    val |= bytes[i] & 0xff;
		  }
		  return val;
		}

	//	pack(InetAddress.getByName(dottedString).getAddress());

		//Int to string:

		byte[] unpack(int bytes) {
		  return new byte[] {
		    (byte)((bytes >>> 24) & 0xff),
		    (byte)((bytes >>> 16) & 0xff),
		    (byte)((bytes >>>  8) & 0xff),
		    (byte)((bytes       ) & 0xff)
		  };
		}


		//InetAddress.getByAddress(unpack(packedBytes)).getHostAddress()


	
	
	private Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			FloodlightContext cntx) throws Exception {

		// Read in packet data headers by using OFMatch
		OFMatch match = new OFMatch();
		match.loadFromPacket(pi.getPacketData(), pi.getInPort());
		
		
		Integer curDestIPAddress = match.getNetworkDestination();
		String curDestIPString  = InetAddress.getByAddress(unpack(curDestIPAddress)).getHostAddress();
		//String curDestIPString = IPv4.fromIPv4Address(curDestIPAddress);
		//String destMACAddress = new String(match.getDataLayerDestination());

		System.out.println("MYD: " + sw.getStringId());
	
		Server destServer = null;
	
		if (curDestIPAddress == LOAD_BALANCER_IP) {
			
			destServer = getDestServer(sw, pi);
			String destIP = destServer.getIP();	
			System.out.println("MYD: S = " + sw.getId());

			short outPort = topology.getNextHop(destIP,"s"+sw.getId());
			destServer.setPort(outPort);
	
		} else {

			short outPort = topology.getNextHop(curDestIPString, "s"+ sw.getId());
			destServer = new Server();
			destServer.setPort(outPort);
			destServer.setIP(curDestIPString);
			destServer.setMAC(topology.getMacAddressFromIP(curDestIPString));
			

		}
		
		processRuleAndPushPacket(destServer, sw, pi);
		return Command.CONTINUE;
	}
	
	private Command processFlowRemovedMessage(IOFSwitch sw,
			OFFlowRemoved flowRemovedMessage) throws IOException,
			InterruptedException, ExecutionException {

		Long sourceMac = Ethernet.toLong(flowRemovedMessage.getMatch()
				.getDataLayerSource());
		Long destMac = Ethernet.toLong(flowRemovedMessage.getMatch()
				.getDataLayerDestination());

		Integer destIP = flowRemovedMessage.getMatch().getNetworkDestination();
		Short appPort = flowRemovedMessage.getMatch().getTransportDestination();

		// Collect Statistics for each flow that was removed
		portServerPacketCountMap.clear();
		Node currentSwitch = topology.getTopology().get(sw.getStringId());
		
		String dstipstr = InetAddress.getByAddress(unpack(destIP)).getHostAddress();
		
		if (topology.isNextHop(currentSwitch,(dstipstr))) {

			Integer count = (int) (portServerPacketCountMap.get(appPort).get(
					(destIP)) + flowRemovedMessage.getPacketCount());
			portServerPacketCountMap.get(appPort).put((dstipstr), count);

		}

		if (logger.isTraceEnabled()) {
			logger.trace("{} flow entry removed {}", sw,
					HexString.toHexString(sourceMac));
		}

		logger.info(
				"{} flow entry removed {} used Bytes:"
						+ flowRemovedMessage.getByteCount(), sw,
				HexString.toHexString(sourceMac));
		logger.info(
				"{} flow entry removed {} used Bytes:"
						+ flowRemovedMessage.getByteCount(), sw,
				HexString.toHexString(destMac));

		return Command.CONTINUE;
	}
	
	
	private Server getDestServer(IOFSwitch sw, OFPacketIn pi) {

		OFMatch match = new OFMatch();
		match.loadFromPacket(pi.getPacketData(), pi.getInPort());

		Short appPort = 8080;//= match.getTransportDestination();
		System.out.println("MYD:appPort"+ appPort.toString());
		// If no stats available use Round Robin
		if (portServerPacketCountMap.isEmpty()) {

			List<Server> servers = portNumberServersMap.get(appPort);
			
			lastServer = (lastServer + 1) % servers.size();
			System.out.println("MYD: Lastserver"+lastServer);
			System.out.println(servers.get(lastServer).getIP());
			return servers.get(lastServer);

		} else { // Get least loaded server

			String destIP = portServerPacketCountMap.get(appPort).firstEntry()
					.getKey();
			Server server = new Server();
			server.setIP(destIP);
			server.setMAC(topology.getMacAddressFromIP(destIP));
			return server;

		}

	}


	private void processRuleAndPushPacket(Server server,IOFSwitch sw, OFPacketIn pi) throws Exception {
		// TODO Implemented round-robin load balancing
		
		
		// Create a flow table modification message to add a rule
    	OFFlowMod rule = new OFFlowMod();
		rule.setType(OFType.FLOW_MOD); 			
		rule.setCommand(OFFlowMod.OFPFC_ADD);
			
		// Create match based on packet
		OFMatch match = new OFMatch();
        match.loadFromPacket(pi.getPacketData(), pi.getInPort());
        
        // Match exact flow -- i.e., no wildcards
		match.setWildcards(~OFMatch.OFPFW_ALL);
		rule.setMatch(match);
			
		// Specify the timeouts for the rule
		rule.setIdleTimeout(IDLE_TIMEOUT);
		rule.setHardTimeout(HARD_TIMEOUT);
	        
	    // Set the buffer id to NONE -- implementation artifact
		rule.setBufferId(OFPacketOut.BUFFER_ID_NONE);
	       
        // Initialize list of actions
		ArrayList<OFAction> actions = new ArrayList<OFAction>();
		
		// Add action to re-write destination MAC to the MAC of the chosen server
		OFAction rewriteMAC = new OFActionDataLayerDestination(server.getMAC().getBytes());
		actions.add(rewriteMAC);
		
		// Add action to re-write destination IP to the IP of the chosen server
		OFAction rewriteIP = new OFActionNetworkLayerDestination(
				pack(InetAddress.getByName(server.getIP()).getAddress())
				);
		actions.add(rewriteIP);
			
		// Add action to output packet
		OFAction outputTo = new OFActionOutput(server.getPort());
		actions.add(outputTo);
		
		// Add actions to rule
		rule.setActions(actions);
		short actionsLength = (short)(OFActionDataLayerDestination.MINIMUM_LENGTH
				+ OFActionNetworkLayerDestination.MINIMUM_LENGTH
				+ OFActionOutput.MINIMUM_LENGTH);
		
		// Specify the length of the rule structure
		rule.setLength((short) (OFFlowMod.MINIMUM_LENGTH + actionsLength));
		
		//logger.debug("Actions length="+ (rule.getLength() - destIPOFFlowMod.MINIMUM_LENGTH));
		
		//logger.debug("Install rule for forward direction for flow: " + rule);
			
		try {
			sw.write(rule, null);
		} catch (Exception e) {
			e.printStackTrace();
		}	

		
		pushPacket(sw, pi, actions, actionsLength);
	}
	
	/**
	 * Sends a packet out to the switch
	 */
	private void pushPacket(IOFSwitch sw, OFPacketIn pi, 
			ArrayList<OFAction> actions, short actionsLength) {
		
		// create an OFPacketOut for the pushed packet
        OFPacketOut po = (OFPacketOut) floodlightProvider.getOFMessageFactory()
                		.getMessage(OFType.PACKET_OUT);        
        
        // Update the inputPort and bufferID
        po.setInPort(pi.getInPort());
        po.setBufferId(pi.getBufferId());
                
        // Set the actions to apply for this packet		
		po.setActions(actions);
		po.setActionsLength(actionsLength);
	        
        // Set data if it is included in the packet in but buffer id is NONE
        if (pi.getBufferId() == OFPacketOut.BUFFER_ID_NONE) {
            byte[] packetData = pi.getPacketData();
            po.setLength(U16.t(OFPacketOut.MINIMUM_LENGTH
                    + po.getActionsLength() + packetData.length));
            po.setPacketData(packetData);
        } else {
            po.setLength(U16.t(OFPacketOut.MINIMUM_LENGTH
                    + po.getActionsLength()));
        }        
        
       // logger.debug("Push packet to switch: "+po);
        
        // Push the packet to the switch
        try {
            sw.write(po, null);
        } catch (IOException e) {
            logger.error("failed to write packetOut: ", e);
        }
	}


}