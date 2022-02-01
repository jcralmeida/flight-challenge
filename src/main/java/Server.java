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



public class Server {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static void main(String[] args) throws InterruptedException, IOException {
    final Location location = Location.forGrpcInsecure("localhost", 4152);


    FlightProducer producer =  new FlightProducer() {
      @Override
      public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {

        final IntVector intVector = new IntVector("id_number", allocator);
        final VarCharVector charVector = new VarCharVector("name_value", allocator);

        intVector.setSafe(0,1);
        intVector.setSafe(1,2);
        intVector.setSafe(2,3);
        intVector.setSafe(3,4);
        charVector.setSafe(0, "Joao".getBytes());
        charVector.setSafe(1, "Pedro".getBytes());
        charVector.setSafe(2, "Luis".getBytes());
        charVector.setSafe(3, "Jose".getBytes());

        List<FieldVector> list = asList(intVector, charVector);



        try(final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(list)) {
          vectorSchemaRoot.setRowCount(4);

          serverStreamListener.start(vectorSchemaRoot);
          serverStreamListener.putNext();
          serverStreamListener.completed();
        }


      }

      @Override
      public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {

      }

      @Override
      public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
        //criar um schema com metadados retornados na query
        //criar um endpoint
        //add endpoint na lista
        //intanciar e retornar um flight info
         final Schema GET_DATA_KEYS = new Schema(asList(
             Field.notNullable("intVector",INT.getType()),
             Field.notNullable("charVector",VARCHAR.getType())));

         final List<FlightEndpoint> endpointList = Collections.singletonList(new FlightEndpoint(
             new Ticket(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))));

        FlightInfo info = new FlightInfo(GET_DATA_KEYS, flightDescriptor, endpointList, -1, -1);
        return info;
      }
      @Override
      public Runnable acceptPut(CallContext callContext, FlightStream flightStream,
                                StreamListener<PutResult> streamListener) {
        return null;
      }

      @Override
      public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {

      }

      @Override
      public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {

      }
    };


    //TODO Uncomment this code
    final FlightServer server = FlightServer.builder(allocator, location, producer).build().start();
      // Print out message for integration test script
    System.out.println("Server listening on localhost:" + server.getPort());

    server.awaitTermination();
  }

}
