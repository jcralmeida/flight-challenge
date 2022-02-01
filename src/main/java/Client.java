import java.nio.charset.StandardCharsets;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

class Client {
  public static final RootAllocator allocator
      = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    FlightClient client = FlightClient.builder()
        .location(location)
        .allocator(allocator)
        .build();
    // Make the call to the getFlightInfo and GetStream
    final FlightInfo flightInfo = client
        .getInfo(FlightDescriptor
            .command("getInfo".getBytes(StandardCharsets.UTF_8)));

    final FlightStream flightStream = client
        .getStream(flightInfo.getEndpoints().get(0).getTicket());

    while (flightStream.next()) {
      System.out.println(flightStream.getRoot().getFieldVectors());
    }

  }

}
