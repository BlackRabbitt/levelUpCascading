package level4;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Created by sujishakya on 3/7/14.
 */
public class wordCount {
    public static FlowDef wordCount(Tap source, Tap sink) {
        Pipe pipe = new Pipe("pipe");
        // this is the simple method for word count.
        pipe = new CountBy(pipe, new Fields("word"), new Fields("count"));

        // or, you can do this to count the word. Just comment the above line and uncomment the following lines.
        /*pipe = new GroupBy(pipe, new Fields("line"), Fields.ALL );
        pipe = new Every(pipe, Fields.ALL, new Count(), Fields.ALL);*/

        return FlowDef.flowDef()
                .addSource(pipe, source)
                .addTail(pipe)
                .addSink(pipe, sink);
    }
}
