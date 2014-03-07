package level0;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 * Created by sujishakya on 3/7/14.
 */
public class CopyFiles {
    public static FlowDef knowTheBasicCopy(Tap sourceTap, Tap sinkTap) {
        // create a pipe
        Pipe copyPipe = new Pipe("copyPipe");

        // define a flow and return
        return FlowDef.flowDef()
                .addSource(copyPipe, sourceTap)
                .addTail(copyPipe)
                .addSink(copyPipe, sinkTap);

    }


}
