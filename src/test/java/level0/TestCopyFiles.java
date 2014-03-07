package level0;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 * Created by sujit shakya on 3/7/14.
 */
public class TestCopyFiles {
    @Test
    public void knowTheBasicTest() throws Exception{
        // define source
        String sourceFile = "src/test/resources/level0.txt";

        Tap sourceTap = new FileTap(new TextLine(new Fields("line")), sourceFile);

        // define sink
        String sinkFile = "src/test/resources/Outlevel0.txt";

        Tap sinkTap = new FileTap(new TextLine(), sinkFile);

        // create a job definition and run it
        FlowDef flowDef = CopyFiles.knowTheBasicCopy(sourceTap, sinkTap);
        new LocalFlowConnector().connect(flowDef).complete();
    }
}
