package main.edu.columbia.cs6998.sdn.project;

import java.io.IOException;
import java.net.InetAddress;
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
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPacket;
import net.floodlightcontroller.packet.IPv4;

import org.openflow.protocol.OFError;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFFlowRemoved;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionNetworkLayerDestination;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.action.OFActionType;
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
	//private final static byte[] LOAD_BALANCER_MAC =  Ethernet.toMACAddress("00:00:00:00:00:FE");
	private final static byte[] LOAD_BALANCER_MAC =  Ethernet.toMACAddress("66:15:95:e3:33:69");

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
		//appServers.add(new Server("10.0.0.5",topology.getMacAddressFromIP("10.0.0.5")));
		/*appServers.add(new Server("10.0.0.6",topology.getMacAddressFromIP("10.0.0.6")));
		appServers.add(new Server("10.0.0.7",topology.getMacAddressFromIP("10.0.0.7")));
		 */
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
			break;

		case FLOW_REMOVED:
			try {
				return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;

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



	/**
	 * Handles incoming ARP requests. Reads the relevant information, creates an ARPRequest
	 * object, sends out the ARP request message or, if the information is already known by
	 * the system, sends back an ARP reply message.
	 *
	 * @param arp The ARP (request) packet received.
	 * @param switchId The ID of the incoming switch where the ARP message is received.
	 * @param portId The Port ID where the ARP message is received.
	 * @param cntx The Floodlight context.
	 * @return <b>Command</b> The command whether another listener should proceed or not.
	 */
	protected Command handleARPRequest(ARP arp, long switchId, short portId, FloodlightContext cntx) {
		/* The known IP address of the ARP source. */
		long sourceIPAddress = IPv4.toIPv4Address(arp.getSenderProtocolAddress());
		/* The known MAC address of the ARP source. */
		long sourceMACAddress = Ethernet.toLong(arp.getSenderHardwareAddress());
		/* The IP address of the (yet unknown) ARP target. */
		long targetIPAddress = IPv4.toIPv4Address(arp.getTargetProtocolAddress());
		/* The MAC address of the (yet unknown) ARP target. */
		long targetMACAddress = Ethernet.toLong(LOAD_BALANCER_MAC);


		IPacket arpReply = new Ethernet()
		.setSourceMACAddress(Ethernet.toByteArray(targetMACAddress))
		.setDestinationMACAddress(Ethernet.toByteArray(sourceMACAddress))
		.setEtherType(Ethernet.TYPE_ARP)
		.setPayload(new ARP()
		.setHardwareType(ARP.HW_TYPE_ETHERNET)
		.setProtocolType(ARP.PROTO_TYPE_IP)
		.setOpCode(ARP.OP_REPLY)
		.setHardwareAddressLength((byte)6)
		.setProtocolAddressLength((byte)4)
		.setSenderHardwareAddress(Ethernet.toByteArray(targetMACAddress))
		.setSenderProtocolAddress(IPv4.toIPv4AddressBytes((int)targetIPAddress))
		.setTargetHardwareAddress(Ethernet.toByteArray(sourceMACAddress))
		.setTargetProtocolAddress(IPv4.toIPv4AddressBytes((int)sourceIPAddress))
		.setPayload(new Data(new byte[] {0x01})));
		// Send ARP reply.
		sendPOMessage(arpReply, floodlightProvider.getSwitch(switchId), 
				portId);
		return Command.CONTINUE;

	}

	/**
	 * Creates and sends an OpenFlow PacketOut message containing the packet
	 * information to the switch. The packet included on the PacketOut message
	 * is sent out at the given port.
	 *
	 * @param packet The packet that is sent out.
	 * @param sw The switch the packet is sent out.
	 * @param port The port the packet is sent out.
	 */
	protected void sendPOMessage(IPacket packet, IOFSwitch sw, short port) {                
		// Serialize and wrap in a packet out
		byte[] data = packet.serialize();
		OFPacketOut po = (OFPacketOut) floodlightProvider.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
		po.setBufferId(OFPacketOut.BUFFER_ID_NONE);
		po.setInPort(OFPort.OFPP_NONE);

		// Set actions
		List<OFAction> actions = new ArrayList<OFAction>();
		actions.add(new OFActionOutput(port, (short) 0));
		po.setActions(actions);
		po.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);

		// Set data
		po.setLengthU(OFPacketOut.MINIMUM_LENGTH + po.getActionsLength() + data.length);
		po.setPacketData(data);

		// Send message
		try {
			sw.write(po, null);
			sw.flush();
		} catch (IOException e) {
			logger.error("Failure sending ARP out port {} on switch {}", new Object[] { port, sw.getStringId() }, e);
		}
	}


	private Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			FloodlightContext cntx) throws Exception {

		// Read in packet data headers by using OFMatch
		OFMatch match = new OFMatch();
		match.loadFromPacket(pi.getPacketData(), pi.getInPort());

		Ethernet ethPacket = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);

		System.out.println(sw.toString());
		System.out.println(ethPacket.toString());


		// Get the ARP packet or continue.
		if (ethPacket.getPayload() instanceof ARP) {
			/* A new empty ARP packet. */
			ARP arp = new ARP();
			arp = (ARP) ethPacket.getPayload();

			if( arp.getOpCode() == ARP.OP_REQUEST ) {


				return this.handleARPRequest(arp, sw.getId(), pi.getInPort(), cntx);
			}
		}	
		else if (ethPacket.getEtherType() == Ethernet.TYPE_IPv4 )
		{	

			Integer curDestIPAddress = match.getNetworkDestination();
			String curDestIPString  = InetAddress.getByAddress(unpack(curDestIPAddress)).getHostAddress();
			//String curDestIPString = IPv4.fromIPv4Address(curDestIPAddress);
			//String destMACAddress = new String(match.getDataLayerDestination());

			Server destServer = null;

			if (curDestIPAddress == LOAD_BALANCER_IP) {

				destServer = getDestServer(sw, pi);
				String destIP = destServer.getIP();

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
		}
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

		Short appPort = match.getTransportDestination();
		// If no stats available use Round Robin
		if (portServerPacketCountMap.isEmpty()) {

			List<Server> servers = portNumberServersMap.get(appPort);
			lastServer = (lastServer + 1) % servers.size();
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
		int actionsLength =  0;
		// Initialize list of actions
		ArrayList<OFAction> actions = new ArrayList<OFAction>();

		// Add action to re-write destination MAC to the MAC of the chosen server
		byte [] b = server.getMAC().getBytes();
		OFActionDataLayerDestination rewriteMAC = new OFActionDataLayerDestination(b);
		System.out.println("RewriteMac "+server.getMAC()+" Byte array "+rewriteMAC.getDataLayerAddress().toString());
		rewriteMAC.setType(OFActionType.SET_DL_DST);
		actions.add(rewriteMAC);
		actionsLength += rewriteMAC.getLengthU()+10;
		System.out.println("LenU "+ actionsLength +" MIN_LEN "+OFAction.MINIMUM_LENGTH);
		// Add action to re-write destination IP to the IP of the chosen server


		OFActionNetworkLayerDestination rewriteIP = new OFActionNetworkLayerDestination(
				pack(InetAddress.getByName(server.getIP()).getAddress())
				);
		rewriteIP.setType( OFActionType.SET_NW_DST);
		actions.add(rewriteIP);
		actionsLength += rewriteIP.getLengthU();
		//System.out.println("actionsU "+ actionsLength+"rewriteIP "+rewriteIP.getLength() +" MIN_LEN "+OFAction.MINIMUM_LENGTH);

		// Add action to output packet
		OFActionOutput outputTo = new OFActionOutput().setPort((short) 3);
		outputTo.setType(OFActionType.OUTPUT );
		actions.add(outputTo);
		actionsLength += outputTo.getLengthU();
		rule.setOutPort((short) 3);

		// Add actions to rule
		rule.setActions(actions);
		/*	short actionsLength = (short)(OFActionDataLayerDestination.MINIMUM_LENGTH
				+ OFActionNetworkLayerDestination.MINIMUM_LENGTH
				+ OFActionOutput.MINIMUM_LENGTH);
		 */
		// Specify the length of the rule structure
		rule.setLengthU(OFFlowMod.MINIMUM_LENGTH + actionsLength);

		//logger.debug("Actions length="+ (rule.getLength() - destIPOFFlowMod.MINIMUM_LENGTH));

		//logger.debug("Install rule for forward direction for flow: " + rule);

		try {
			sw.write(rule, null);
		} catch (Exception e) {
			e.printStackTrace();
		}	


		//pushPacket(sw, pi, actions, actionsLength);
	}

	/**
	 * Sends a packet out to the switch
	 */
	private void pushPacket(IOFSwitch sw, OFPacketIn pi, 
			ArrayList<OFAction> actions, int actionsLength) {

		// create an OFPacketOut for the pushed packet
		OFPacketOut po = (OFPacketOut) floodlightProvider.getOFMessageFactory()
				.getMessage(OFType.PACKET_OUT);        

		// Update the inputPort and bufferID
		po.setInPort(pi.getInPort());
		po.setBufferId(pi.getBufferId());

		// Set the actions to apply for this packet		
		po.setActions(actions);
		po.setActionsLength((short) actionsLength);

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
