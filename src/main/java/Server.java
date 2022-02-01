

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
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

        List<FieldVector> list = Arrays.asList(intVector, charVector);
        int rows = list.size();


        try(final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(list)) {
          vectorSchemaRoot.setRowCount(rows);

          serverStreamListener.start((VectorSchemaRoot) list);
          serverStreamListener.putNext();
        }
        serverStreamListener.completed();

        System.out.println(list);
      }

      @Override
      public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {

      }

      @Override
      public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
        return null;

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
