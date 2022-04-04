import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;

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
