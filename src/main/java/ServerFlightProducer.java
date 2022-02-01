import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

public class ServerFlightProducer implements FlightProducer {

  RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
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

      List<Field> fields = Arrays.asList(intVector.getField(), varCharVector.getField());
      List<FieldVector> productsList = Arrays.asList(intVector, varCharVector);
      int rows = productsList.size();

      try(final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, productsList)){
        vectorSchemaRoot.setRowCount(rows);
        serverStreamListener.start((VectorSchemaRoot) productsList);
        serverStreamListener.putNext();
      }
      serverStreamListener.completed();
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {

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
}
