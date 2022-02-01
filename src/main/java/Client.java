import java.nio.charset.StandardCharsets;

import javax.xml.ws.EndpointContext;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

public class Client{
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  public static EndpointContext flightInfo;

  public static void main(String[] args){
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    FlightClient client = FlightClient.builder()
        .location(location)
        .allocator(allocator)
        .build();

    // Make the call to the getFlightInfo and GetStream
    FlightDescriptor flight_desc = FlightDescriptor.command("GetFlightInfo".getBytes(StandardCharsets.UTF_8));
    final FlightInfo flightInfo = client.getInfo(flight_desc);
    System.out.println("EndPoints: " + flightInfo.getEndpoints() + "Descriptor: "+ flightInfo.getDescriptor());
    System.out.println("Ticket: " + flightInfo.getEndpoints().get(0).getTicket());
    final FlightStream flightStream = client.getStream(flightInfo.getEndpoints().get(0).getTicket());

  }
}