package level2;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Created by sujishakya on 3/7/14.
 */
public class BufferOperation{
    public static FlowDef bufferOperation(Tap sourceTap, Tap sinkTap) {
        Pipe bufferPipe = new Pipe("Buffer");
        // Group By according to the id
        bufferPipe = new GroupBy(bufferPipe, new Fields("id"));
        // for every group apply the bufferOperate() operation.
        bufferPipe = new Every(bufferPipe, new bufferOperate(new Fields("id", "value")));
        return FlowDef.flowDef()
                .addSource(bufferPipe, sourceTap)
                .addSink(bufferPipe, sinkTap)
                .addTail(bufferPipe);
    }
}
