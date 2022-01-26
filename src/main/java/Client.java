import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

public class Client {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    FlightClient client = FlightClient.builder()
        .location(location)
        .allocator(allocator)
        .build();

    // Make the call to the getFlightInfo and GetStream
  }
}
