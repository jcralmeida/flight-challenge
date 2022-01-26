import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

public class Server {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) throws InterruptedException {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    // Instantiate here the Producer you've created.
    FlightProducer producer;


    //TODO Uncomment this code
//    final FlightServer server = FlightServer.builder(allocator, location, producer).build().start();
//    // Print out message for integration test script
//    System.out.println("Server listening on localhost:" + server.getPort());
//
//    server.awaitTermination();
  }

}
