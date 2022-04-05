import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;

public class Server {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) throws InterruptedException, IOException {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    // Instantiate here the Producer you've created.
    FlightProducer producer = new ExampleFlightProducer();

    final FlightServer server = FlightServer.builder(allocator, location, producer).build().start();
    // Print out message for integration test script
    System.out.println("Server listening on localhost:" + server.getPort());

    server.awaitTermination();
  }

}
