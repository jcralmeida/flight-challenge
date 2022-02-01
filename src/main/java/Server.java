import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;
import org.apache.arrow.memory.RootAllocator;

public class Server {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) throws InterruptedException {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    // Instantiate here the Producer you've created.
    ServerFlightProducer serverFlightProducer = new ServerFlightProducer();

    //TODO Uncomment this code
    final FlightServer server;
    try {
      server = FlightServer.builder(allocator, location, serverFlightProducer)
          .build()
          .start();
      // Print out message for integration test script
      System.out.println("Server listening on localhost:" + server.getPort());
      server.awaitTermination();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
