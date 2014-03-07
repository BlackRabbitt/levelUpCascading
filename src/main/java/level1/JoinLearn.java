package level1;

import cascading.flow.FlowDef;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Created by sujit shakya on 3/7/14.
 */
public class JoinLearn {
    public static FlowDef JoinFiles(Tap source1Tap, Tap source2Tap, Tap sinkTap) {
        // create a left pipe and right pipe before joining.
        Pipe leftPipe = new Pipe("leftPipe");
        Pipe rightPipe = new Pipe("rightPipe");

        // You have to rename the common fields before using CoGroup.
        rightPipe = new Rename(rightPipe, new Fields("id"), new Fields("s_id"));

        // output pipe after join
        Pipe jointPipe ;

        // In this level you do leftJoin but there are other option like rightJoin and InnerJoin just like in SQLJoin, (If you are familier with).
        // You can always refer to userguide [cascading.org] for further details and information
        jointPipe = new CoGroup(leftPipe, new Fields("id"), rightPipe, new Fields("s_id"),new LeftJoin());
        return FlowDef.flowDef()
                .addSource(leftPipe, source1Tap)
                .addSource(rightPipe, source2Tap)
                .addTail(jointPipe)
                .addSink(jointPipe, sinkTap);

    }
}
