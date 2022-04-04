import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;


import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  static Logger logger = Logger.getLogger(Client.class.getName());

  public static void main(String[] args) {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    FlightClient client = FlightClient.builder()
        .location(location)
        .allocator(allocator)
        .build();
    FlightDescriptor flightDescriptor = FlightDescriptor.command("SELECT * FROM table".getBytes(StandardCharsets.UTF_8));
    FlightInfo info = client.getInfo(flightDescriptor);
    FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    while (stream.next()){
      VectorSchemaRoot root = stream.getRoot();
      for (FieldVector fieldVector : root.getFieldVectors()) {
        for (int i = 0; i < root.getRowCount(); i ++){
          System.out.println(fieldVector.getObject(i));
        }
      }
    }

    // Make the call to the getFlightInfo and GetStream
  }
}
