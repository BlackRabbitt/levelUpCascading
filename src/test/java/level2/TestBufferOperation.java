package level2;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import level0.CopyFiles;
import org.junit.Test;

/**
 * Created by sujishakya on 3/7/14.
 */
public class TestBufferOperation {
    @Test
    public void TestBuffer() throws Exception{
        // define source
        String sourceFile = "src/test/resources/level2.csv";

        Tap sourceTap = new FileTap(new TextDelimited(new Fields("id","value"), ";"), sourceFile);

        // define sink
        String sinkFile = "src/test/resources/Outlevel2.csv";

        Tap sinkTap = new FileTap(new TextDelimited(false, "\t"), sinkFile);

        // create a job definition and run it
        FlowDef flowDef = BufferOperation.bufferOperation(sourceTap, sinkTap);
        new LocalFlowConnector().connect(flowDef).complete();
    }
}
