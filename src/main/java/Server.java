import static java.util.Arrays.asList;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Class Initializer Server.
 */
public class Server {

  public static final RootAllocator allocator
      = new RootAllocator(Long.MAX_VALUE);

  /** Main method.
   *
   * @param args params.
   * @throws InterruptedException exception throw.
   * @throws IOException IOException throw.
   */
  public static void main(String[] args)
      throws InterruptedException, IOException {
    final Location location = Location.forGrpcInsecure("localhost", 4152);

    FlightProducer producer =  new FlightProducer() {
      @Override
      public void getStream(CallContext callContext, final Ticket ticket,
                            final ServerStreamListener serverStreamListener) {
        final IntVector intVector = new IntVector("Id", allocator);
        final VarCharVector varCharVector = new VarCharVector("Product", allocator);

        intVector.allocateNew();
        varCharVector.allocateNew();

        // Id Values
        intVector.setSafe(0, 1);
        intVector.setSafe(1, 2);
        intVector.setSafe(2, 3);
        intVector.setSafe(3, 4);
        // Product Values
        varCharVector.setSafe(0, "Rice".getBytes(StandardCharsets.UTF_8));
        varCharVector.setSafe(1, "Bean".getBytes(StandardCharsets.UTF_8));
        varCharVector.setSafe(2, "Beef".getBytes(StandardCharsets.UTF_8));
        varCharVector.setSafe(3, "Potato".getBytes(StandardCharsets.UTF_8));

        List<FieldVector> productsList = asList(intVector, varCharVector);
        try (VectorSchemaRoot vectorSchemaRoot
                = new VectorSchemaRoot(productsList)) {
          vectorSchemaRoot.setRowCount(4);
          serverStreamListener.start((vectorSchemaRoot));
          serverStreamListener.putNext();
          serverStreamListener.completed();
        }
      }

      @Override
      public void listFlights(
          final CallContext callContext,
          final Criteria criteria,
          final StreamListener<FlightInfo> streamListener) {
      }

      @Override
      public FlightInfo getFlightInfo(final CallContext callContext, final FlightDescriptor flightDescriptor) {
        final Schema schema = new Schema(asList(
            Field.notNullable("intVector", INT.getType()),
            Field.notNullable("varCharVector", VARCHAR.getType())));

        final List<FlightEndpoint> endpointList = Collections.singletonList(new FlightEndpoint(
                new Ticket(UUID.randomUUID()
                .toString()
                .getBytes(StandardCharsets.UTF_8))));

        return new FlightInfo(schema, flightDescriptor, endpointList, -1, -1);
      }

      @Override
      public Runnable acceptPut(
          final CallContext callContext,
          final FlightStream flightStream,
          final StreamListener<PutResult> streamListener) {
        return null;
      }

      @Override
      public void doAction(
          final CallContext callContext,
          final Action action,
          final StreamListener<Result> streamListener) {
      }

      @Override
      public void listActions(
          final CallContext callContext,
          final StreamListener<ActionType> streamListener) {
      }
    };

    //TODO Uncomment this code
    final FlightServer server = FlightServer
        .builder(allocator, location, producer)
        .build()
        .start();
    // Print out message for integration test script
    System.out.println("Server listening on localhost:" + server.getPort());
    server.awaitTermination();
  }
}
