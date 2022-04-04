import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Arrays.asList;

public class ExampleFlightProducer implements FlightProducer {
    static private final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    @Override
    public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
        String str = "SELECT * FROM table";
        if (Arrays.equals(str.getBytes(), ticket.getBytes())){
            final VectorSchemaRoot vectorSchemaRoot = createVectors();
            vectorSchemaRoot.setRowCount(3);
            serverStreamListener.start(vectorSchemaRoot);
            serverStreamListener.putNext();
            serverStreamListener.completed();
        }
    }

    private VectorSchemaRoot createVectors(){
        VarCharVector vecStr = new VarCharVector("str", rootAllocator);
        for (int i = 0 ; i< 3 ; i++){
            vecStr.setSafe(i, new Text("string"+ i));
        }
        IntVector vecInt = new IntVector("int", rootAllocator);
        for (int i = 0 ; i< 3 ; i++){
            vecInt.setSafe(i, i*50);
        }
        return new VectorSchemaRoot(asList(vecStr, vecInt));
    }

    @Override
    public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {

    }

    @Override
    public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
        Ticket ticket = new Ticket(flightDescriptor.getCommand());
        Schema schema = new Schema(asList(
                Field.nullable("int", Types.MinorType.INT.getType()),
                Field.nullable("str", Types.MinorType.VARCHAR.getType())
        ));
        FlightEndpoint flightEndpoint = new FlightEndpoint(ticket);
        return new FlightInfo(schema, flightDescriptor, Collections.singletonList(flightEndpoint), -1, -1);
    }

    @Override
    public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
        return FlightProducer.super.getSchema(context, descriptor);
    }

    @Override
    public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener) {
        return null;
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        FlightProducer.super.doExchange(context, reader, writer);
    }

    @Override
    public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {

    }

    @Override
    public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {

    }
}
