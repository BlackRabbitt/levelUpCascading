package level2;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Created by sujishakya on 3/7/14.
 */
public class bufferOperate extends BaseOperation<TupleEntry> implements Buffer<TupleEntry>  {
    public bufferOperate(Fields comparables) {
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall<TupleEntry> bufferCall) {
        // get the group values for the current grouping
        TupleEntry group = bufferCall.getGroup();

        // get the current argument values for this grouping
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

        // create a tuple to hold our result values
        Tuple result = new Tuple();
        while ( arguments.hasNext() ){
            // save each group to argument tuple
            TupleEntry argument = arguments.next();

            // append the value to temporary string
            String temp = new String();
            temp += argument.getString("value");

            // add the string into the tuple result.
            result.add(temp);
        }
        bufferCall.getOutputCollector().add(result);
    }
}
